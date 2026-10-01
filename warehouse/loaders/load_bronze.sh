#!/usr/bin/env bash
# Load the generated Nimbus data into the bronze tables — the muscle behind
# `make wh-generate`. Kept as a script (not an inline make recipe) because the loading
# has to be defensive about a deliberately memory-constrained k3d cluster:
#
#   * The ClickHouse pods are capped at 1.5Gi (max_server_memory_usage ~1.35 GiB), so
#     big inserts trip MEMORY_LIMIT_EXCEEDED. We stream CSVs in row-chunks and run the
#     native INSERT ... SELECTs in row-range BATCHES, so no single statement needs much
#     memory, and we JEMALLOC PURGE + retry on the occasional transient pressure blip.
#   * `kubectl exec -i` streaming over k3d times out on long/large transfers, so every
#     transfer is kept short by the same chunking/batching.
#
# All config comes from the environment (set by scripts/warehouse.mk). Loads run as
# `admin` — this is DDL/bulk-load bootstrap, not a dbt model.
set -euo pipefail

: "${CH_NS:?}" "${CH_POD:?}" "${CH_USER:?}" "${CH_PW:?}" "${LOADERS_DIR:?}" "${GEN_OUT:?}"
N_APP_EVENTS="${N_APP_EVENTS:?}"
N_CARD_AUTHS="${N_CARD_AUTHS:?}"
BATCH="${BATCH:-250000}"        # rows per native INSERT ... SELECT
CSV_CHUNK="${CSV_CHUNK:-250000}" # rows per CSV insert
RETRIES="${RETRIES:-8}"
# Insert block size trades insert peak-memory against part fragmentation. The bronze
# streams are PARTITION BY toYYYYMM over 18 months, so a block spanning the window
# writes up to 18 parts (one per month): tiny 50k blocks => hundreds of tiny parts =>
# a background-merge storm the ~1.35 GiB server cap can't absorb. 250k-row blocks give
# each of the 18 partitions a few larger parts instead, so far fewer merges are needed
# (and the reduced merge pool in the CHI local overlay never falls behind -> no "too
# many parts"). A single 250k block still peaks well under the per-query memory cap.
IBS="--min_insert_block_size_rows=250000 --min_insert_block_size_bytes=0 --max_insert_block_size=250000"

WORK="$(mktemp -d)"
ERRF="$WORK/err"
trap 'rm -rf "$WORK"' EXIT

PY_TABLES=(raw_customers raw_kyc_events raw_accounts raw_account_events raw_cards raw_ledger_postings)
ALL_TABLES=("${PY_TABLES[@]}" raw_app_events raw_card_authorizations)

ch()   { kubectl -n "$CH_NS" exec    "$CH_POD" -c clickhouse -- clickhouse-client -u "$CH_USER" --password "$CH_PW" "$@"; }
ch_i() { kubectl -n "$CH_NS" exec -i "$CH_POD" -c clickhouse -- clickhouse-client -u "$CH_USER" --password "$CH_PW" "$@"; }
# Release allocator-retained memory and ClickHouse's own caches — cheap relief that
# lowers the tracked footprint between batches. (OS page cache is the kernel's to
# reclaim; the small blocks above keep us from leaning on it.)
purge() {
  ch -q "SYSTEM JEMALLOC PURGE ON CLUSTER '{cluster}'" >/dev/null 2>&1 || true
  ch -q "SYSTEM DROP MARK CACHE" >/dev/null 2>&1 || true
  ch -q "SYSTEM DROP UNCOMPRESSED CACHE" >/dev/null 2>&1 || true
}

# retry <desc> <cmd...> — retry on transient memory / connection errors, fail on the rest.
retry() {
  local desc="$1"; shift
  local i
  for ((i=1; i<=RETRIES; i++)); do
    if "$@" 2>"$ERRF"; then return 0; fi
    if grep -qiE 'MEMORY_LIMIT|i/o timeout|connection|closed network' "$ERRF"; then
      echo "      ($desc) transient failure, purge + retry $i/$RETRIES"
      purge; sleep 3
    else
      echo "!! ($desc) fatal error:" >&2; cat "$ERRF" >&2; return 1
    fi
  done
  echo "!! ($desc) gave up after $RETRIES tries:" >&2; cat "$ERRF" >&2; return 1
}

# Load one header-stripped CSV chunk into a table (FORMAT CSV — column order matches DDL).
load_chunk() { ch_i --input_format_parallel_parsing=0 $IBS -q "INSERT INTO nimbus_raw.$2 FORMAT CSV" < "$1"; }

# Run one native loader batch (offset,count substituted) with a bounded memory footprint.
native_batch() { sed "s/__OFFSET__/$2/g; s/__COUNT__/$3/g" "$1" \
                   | ch_i --max_threads=1 --max_block_size=262144 $IBS --multiquery; }

truncate_one() { ch -q "TRUNCATE TABLE IF EXISTS nimbus_raw.$1 ON CLUSTER '{cluster}'" >/dev/null; }

echo "==> Truncating bronze tables ON CLUSTER..."
for t in "${ALL_TABLES[@]}"; do retry "truncate $t" truncate_one "$t"; done
purge

echo "==> [tier 1] Loading Python CSVs (chunked, streaming)..."
for t in "${PY_TABLES[@]}"; do
  f="$GEN_OUT/$t.csv"
  tail -n +2 "$f" | split -l "$CSV_CHUNK" - "$WORK/${t}_"   # strip header, split body
  n=0
  for chunk in "$WORK/${t}_"*; do
    retry "$t chunk" load_chunk "$chunk" "$t"
    n=$((n + 1))
  done
  echo "    -> nimbus_raw.$t ($n chunk(s))"
done

echo "==> [tier 3] app events: $N_APP_EVENTS rows in batches of $BATCH..."
off=0
while [ "$off" -lt "$N_APP_EVENTS" ]; do
  rem=$((N_APP_EVENTS - off)); cnt=$((rem < BATCH ? rem : BATCH))
  retry "app_events@$off" native_batch "$LOADERS_DIR/10_gen_app_events.sql" "$off" "$cnt"
  off=$((off + BATCH))
done

echo "==> [tier 3] card auths: $N_CARD_AUTHS rows in batches of $BATCH..."
off=0
while [ "$off" -lt "$N_CARD_AUTHS" ]; do
  rem=$((N_CARD_AUTHS - off)); cnt=$((rem < BATCH ? rem : BATCH))
  retry "card_auths@$off" native_batch "$LOADERS_DIR/20_gen_card_auths.sql" "$off" "$cnt"
  off=$((off + BATCH))
done

echo "==> [tier 3] card auth duplicates (~3%, once)..."
retry "card_auth_dupes" bash -c 'cat "$1" | kubectl -n "$CH_NS" exec -i "$CH_POD" -c clickhouse -- \
  clickhouse-client -u "$CH_USER" --password "$CH_PW" --max_insert_block_size=100000 \
  --min_insert_block_size_rows=100000 --min_insert_block_size_bytes=0 --multiquery' _ "$LOADERS_DIR/21_gen_card_auth_dupes.sql"

echo "==> Bronze row counts (nimbus_raw):"
for t in "${ALL_TABLES[@]}"; do printf '    %-28s ' "$t"; ch -q "SELECT count() FROM nimbus_raw.$t"; done
