# dbt-runner image — the P3 "Mode B" (in-cluster) execution of the Nimbus warehouse.
#
# This image bakes in the dbt project and the dbt-clickhouse adapter so a Flux-managed
# CronJob/Job can run `dbt build` / `dbt test` INSIDE the cluster, talking to the CHI
# service directly (no host, no port-forward). The host dev-loop ("Mode A", via
# warehouse/.venv + `make wh-portforward`) is retained as a convenience only.
#
# Build context is the REPO ROOT (the COPY path is repo-relative), so build with:
#   docker build -t localhost:5050/nimbus/dbt-runner:local -f warehouse/Dockerfile .
# (wired as `make wh-image`). See .dockerignore for what is kept out of the context.
#
# No secrets are baked in: the ClickHouse host and the `dbt` password are read from the
# environment at runtime (NIMBUS_CH_HOST / NIMBUS_CH_PASSWORD), which the committed
# profiles/profiles.yml already resolves via env_var(). The CronJob supplies them.
FROM python:3.12-slim

# Pinned to the SAME adapter version the host venv resolves (see `make wh-setup` /
# scripts/warehouse.mk), so host and in-cluster runs use one dbt-clickhouse. Pulls in
# dbt-core + clickhouse-connect transitively.
RUN pip install --no-cache-dir "dbt-clickhouse==1.10.1"

# The dbt project (models, profiles, macros, seeds, tests). Nothing else is needed at
# runtime — bronze data is loaded separately (P1), not by this image.
COPY warehouse/dbt /app/dbt

WORKDIR /app/dbt
# dbt finds the profile here without a --profiles-dir flag, so the default command works.
ENV DBT_PROFILES_DIR=/app/dbt/profiles

# Default action is a full build (seeds + models + tests). The dbt-tester CronJob
# overrides this with `dbt test`; `make wh-build`/`wh-test` create Jobs from those.
CMD ["dbt", "build"]
