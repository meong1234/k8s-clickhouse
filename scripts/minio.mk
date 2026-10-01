# MinIO (in-cluster S3) helpers
# The object-store side of the CAS experiment (docs/cas-local-s3-plan.md). This layer
# is deliberately self-contained: if MinIO fails the Phase A probe below, the backend
# is swapped here and the CHI never notices - which is exactly what happened: MinIO
# ignores If-Match on DeleteObject, so the backend is now Silo.
#
# The probe script is embedded as MINIO_PROBE_SCRIPT at the bottom rather than kept as
# scripts/minio-probe.sh, because the root Makefile does `include scripts/*` - any file
# dropped in this directory is parsed as a makefile.

.PHONY: minio-help minio-status minio-console minio-ls minio-probe

# Must match kubernetes/analytics/minio/{base,local}. MINIO_MC_IMAGE and CURL_IMAGE
# come from scripts/images.mk (shared via `include scripts/*` in the Makefile), so the
# ephemeral pods below use the images already preloaded into the local registry.
MINIO_NS        ?= minio
MINIO_BUCKET    ?= clickhouse-cas
MINIO_SVC       ?= minio
MINIO_ENDPOINT  ?= http://minio.minio.svc.cluster.local:9000

# Local/dev only - see kubernetes/analytics/minio/local/minio-credentials.yaml.
MINIO_USER      ?= minioadmin
MINIO_PASSWORD  ?= minioadmin123

minio-help:
	@echo "MinIO / CAS object store Commands:"
	@echo "---------------------------------"
	@echo "  minio-status     - Show the MinIO pod, PVC, service and the bucket Job"
	@echo "  minio-console    - Port-forward the MinIO console on :9001 and print the creds"
	@echo "  minio-ls         - List the $(MINIO_BUCKET) bucket (object count + bytes)"
	@echo "  minio-probe      - Phase A gate: prove conditional PUTs/DELETEs + ranged reads work"
	@echo ""

minio-status:
	@echo "==> Pods:"
	@kubectl -n $(MINIO_NS) get pods -o wide
	@echo "==> Persistent Volume Claims:"
	@kubectl -n $(MINIO_NS) get pvc
	@echo "==> Services:"
	@kubectl -n $(MINIO_NS) get svc
	@echo "==> Bucket Job (expect Complete):"
	@kubectl -n $(MINIO_NS) get job minio-create-bucket 2>/dev/null || echo "  (not created yet)"

# The console has no Ingress (nothing in this cluster does) - port-forward on demand.
minio-console:
	@echo "==> MinIO console: http://localhost:9001   user=$(MINIO_USER)  password=$(MINIO_PASSWORD)"
	@kubectl -n $(MINIO_NS) port-forward svc/$(MINIO_SVC) 9001:9001

# Object count + total bytes for the bucket. This is the number Phase B compares
# against ONE replica's sum(bytes_on_disk) to prove the blobs are shared, not doubled.
minio-ls:
	@kubectl -n $(MINIO_NS) run minio-ls-$$$$ --rm -i --restart=Never --quiet \
		--image=$(MINIO_MC_IMAGE) --env MC_CONFIG_DIR=/tmp/.mc --command -- \
		sh -c 'mc alias set local $(MINIO_ENDPOINT) $(MINIO_USER) $(MINIO_PASSWORD) > /dev/null && mc ls --recursive --summarize local/$(MINIO_BUCKET)'

# Phase A gate. Runs from INSIDE the cluster (same network path and credentials the
# CAS disk will use); runs every check, then exits non-zero if any failed, so it gates.
minio-probe:
	@echo "==> Phase A probe: $(MINIO_ENDPOINT)/$(MINIO_BUCKET)"
	@printf '%s\n' "$$MINIO_PROBE_SCRIPT" | kubectl -n $(MINIO_NS) run minio-probe-$$$$ \
		--rm -i --restart=Never --quiet --image=$(CURL_IMAGE) \
		--env ENDPOINT=$(MINIO_ENDPOINT) --env BUCKET=$(MINIO_BUCKET) \
		--env CAS_S3_ACCESS_KEY_ID=$(MINIO_USER) \
		--env CAS_S3_SECRET_ACCESS_KEY=$(MINIO_PASSWORD) \
		--command -- sh -s

define MINIO_PROBE_SCRIPT
# Phase A gate (docs/cas-local-s3-plan.md §2): does this S3 implementation actually
# honour the primitives CAS coordinates with? Run via `make minio-probe`, which pipes
# this into an ephemeral curl pod INSIDE the cluster — the same network path and the
# same credentials ClickHouse will use, so a pass here means something.
#
# curl (not mc) because mc exposes no way to send If-None-Match / If-Match on a PUT;
# `--aws-sigv4` makes curl sign the request itself, custom headers included.
#
# Not a unit test of the backend: these checks are exactly what ClickHouse's CAS
# boot-time capability probe asserts. `skip_access_check` must stay false, so a
# failure here is a failure at server start too.
set -u

ENDPOINT="$${ENDPOINT:-http://minio.minio.svc.cluster.local:9000}"
BUCKET="$${BUCKET:-clickhouse-cas}"

# Random key per run so a half-cleaned previous run can never make this one pass.
SUFFIX="$$(date +%s)-$$(tr -dc 'a-z0-9' < /dev/urandom | head -c 8)"
KEY="probe/$${SUFFIX}"
URL="$${ENDPOINT}/$${BUCKET}/$${KEY}"

CURL="curl -sS --aws-sigv4 aws:amz:us-east-1:s3 --user $${CAS_S3_ACCESS_KEY_ID}:$${CAS_S3_SECRET_ACCESS_KEY}"

FAILED=0
check() {
  # check <label> <expected> <actual>
  if [ "$$2" = "$$3" ]; then
    echo "PASS  $$1 (got $$3)"
  else
    echo "FAIL  $$1 (expected $$2, got $$3)"
    FAILED=1
  fi
}

printf 'cas-probe-v1\n' > /tmp/v1.bin
printf 'cas-probe-v2\n' > /tmp/v2.bin

echo "==> object: $${URL}"

# --- a) If-None-Match: * — the create-if-absent primitive CAS uses to publish a blob
#        exactly once. The SECOND writer must lose, or two replicas could both think
#        they own the same content hash.
CODE="$$($$CURL -o /dev/null -w '%{http_code}' -T /tmp/v1.bin -H 'If-None-Match: *' "$$URL")"
check "a1  PUT If-None-Match:* on absent object -> 200" "200" "$$CODE"
CODE="$$($$CURL -o /dev/null -w '%{http_code}' -T /tmp/v1.bin -H 'If-None-Match: *' "$$URL")"
check "a2  PUT If-None-Match:* on existing object -> 412" "412" "$$CODE"

# --- b) If-Match: <etag> — compare-and-swap, how CAS renews mount leases without a
#        lock service. A stale ETag must be rejected, not silently overwritten.
ETAG="$$($$CURL -I "$$URL" | tr -d '\r' | awk -F': ' 'tolower($$1)=="etag" {print $$2}')"
echo "==> current ETag: $${ETAG:-<none>}"
if [ -z "$$ETAG" ]; then
  echo "FAIL  b0  HEAD returned no ETag"
  FAILED=1
fi
CODE="$$($$CURL -o /dev/null -w '%{http_code}' -T /tmp/v2.bin \
        -H 'If-Match: "00000000000000000000000000000000"' "$$URL")"
check "b1  PUT If-Match:<stale etag> -> 412" "412" "$$CODE"
CODE="$$($$CURL -o /dev/null -w '%{http_code}' -T /tmp/v2.bin -H "If-Match: $${ETAG}" "$$URL")"
check "b2  PUT If-Match:<current etag> -> 200" "200" "$$CODE"

# --- c) Ranged GET + a stable object token. CAS reads granules out of the middle of a
#        blob and uses the ETag as the identity of the bytes it cached, so a server
#        that re-derives the ETag per read would silently invalidate every cache entry.
CODE="$$($$CURL -o /tmp/r1 -D /tmp/h1 -w '%{http_code}' -H 'Range: bytes=0-3' "$$URL")"
check "c1  GET Range: bytes=0-3 -> 206" "206" "$$CODE"
CODE="$$($$CURL -o /tmp/r2 -D /tmp/h2 -w '%{http_code}' -H 'Range: bytes=0-3' "$$URL")"
check "c2  GET Range: bytes=0-3 (again) -> 206" "206" "$$CODE"
E1="$$(tr -d '\r' < /tmp/h1 | awk -F': ' 'tolower($$1)=="etag" {print $$2}')"
E2="$$(tr -d '\r' < /tmp/h2 | awk -F': ' 'tolower($$1)=="etag" {print $$2}')"
check "c3  ETag stable across ranged reads" "$$E1" "$$E2"
check "c4  ranged body is 4 bytes" "4" "$$(wc -c < /tmp/r1 | tr -d ' ')"

# --- e) Conditional DELETE (If-Match). THE check this probe was missing: CAS's
#        boot-time capability probe tries to remove an object with a stale
#        incarnation and demands a rejection, because a mount lease is released by
#        deleting the token it holds — a backend that deletes unconditionally lets a
#        fenced server drop the live owner's lease. MinIO accepts the delete and
#        ClickHouse refuses to open the pool ("remove with a stale incarnation was
#        not rejected"), which is why this runs against Silo now. Own key: the
#        checks above need $$KEY to survive to d1.
KEY_D="probe/$${SUFFIX}-del"
URL_D="$${ENDPOINT}/$${BUCKET}/$${KEY_D}"
$$CURL -o /dev/null -T /tmp/v1.bin "$$URL_D"
ETAG_D="$$($$CURL -I "$$URL_D" | tr -d '\r' | awk -F': ' 'tolower($$1)=="etag" {print $$2}')"
echo "==> delete-probe object: $${URL_D}  ETag: $${ETAG_D:-<none>}"

CODE="$$($$CURL -o /dev/null -w '%{http_code}' -X DELETE \
        -H 'If-Match: "00000000000000000000000000000000"' "$$URL_D")"
check "e1a DELETE If-Match:<stale etag> -> 412" "412" "$$CODE"
# A 412 that still deleted the object would be worse than an honest 200, so the
# survival of the object is a separate assertion, not a corollary.
CODE="$$($$CURL -o /dev/null -w '%{http_code}' "$$URL_D")"
check "e1b object survived the rejected DELETE -> GET 200" "200" "$$CODE"

CODE="$$($$CURL -o /dev/null -w '%{http_code}' -X DELETE -H "If-Match: $${ETAG_D}" "$$URL_D")"
check "e2  DELETE If-Match:<current etag> -> 204" "204" "$$CODE"

# --- e3) Batch DeleteObjects with a per-item <ETag>. INFO ONLY: Silo (like MinIO)
#         ignores per-item ETags in the bulk path, and CAS probes bulk-delete
#         capability separately and falls back to single conditional deletes. Recorded
#         because it decides whether GC needs one round or many (plan §4, Phase C).
KEY_B="probe/$${SUFFIX}-batch"
URL_B="$${ENDPOINT}/$${BUCKET}/$${KEY_B}"
$$CURL -o /dev/null -T /tmp/v1.bin "$$URL_B"
cat > /tmp/del.xml <<XML
<Delete><Object><Key>$${KEY_B}</Key><ETag>"00000000000000000000000000000000"</ETag></Object></Delete>
XML
# S3 requires an integrity header on DeleteObjects; curl's --aws-sigv4 only signs the
# payload hash, so compute Content-MD5 here or the request never reaches the handler.
# md5sum | xxd -r -p rather than openssl: the curl image ships no openssl binary.
MD5="$$(md5sum /tmp/del.xml | cut -d' ' -f1 | xxd -r -p | base64)"
BODY="$$($$CURL -o /tmp/del.out -w '%{http_code}' -X POST -H 'Content-Type: application/xml' \
        -H "Content-MD5: $${MD5}" --data-binary @/tmp/del.xml "$${ENDPOINT}/$${BUCKET}/?delete")"
echo "INFO  e3  POST ?delete with a wrong per-item <ETag> -> HTTP $${BODY}"
echo "INFO  e3  body: $$(tr -d '\n' < /tmp/del.out | cut -c1-400)"
CODE="$$($$CURL -o /dev/null -w '%{http_code}' "$$URL_B")"
echo "INFO  e3  object after the batch delete -> GET $${CODE} (404 = per-item ETag ignored)"
$$CURL -o /dev/null -X DELETE "$$URL_B" >/dev/null 2>&1 || true

# --- d) Clean up so the bucket byte count stays a meaningful CAS measurement in
#        Phase B (`make minio-ls` is compared against sum(bytes_on_disk)).
CODE="$$($$CURL -o /dev/null -w '%{http_code}' -X DELETE "$$URL")"
check "d1  DELETE probe object -> 204" "204" "$$CODE"

echo
if [ "$$FAILED" -eq 0 ]; then
  echo "PROBE RESULT: PASS - Gate A satisfied, the backend honours CAS's conditional writes"
else
  echo "PROBE RESULT: FAIL - do NOT configure the CAS disk; see plan §5 for backends"
fi
exit "$$FAILED"
endef
export MINIO_PROBE_SCRIPT
