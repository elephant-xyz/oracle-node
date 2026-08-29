FROM node:22-bookworm-slim

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates unzip \
  && rm -rf /var/lib/apt/lists/*

COPY package.json package-lock.json ./
COPY prepare ./prepare
COPY workflow ./workflow
COPY workflow-events ./workflow-events
COPY budget-alert ./budget-alert
COPY scripts ./scripts
COPY catalog ./catalog
COPY .agents/skills/overture-places-ingest/config/hosted-service-categories.txt \
  ./.agents/skills/overture-places-ingest/config/hosted-service-categories.txt

RUN npm ci --no-audit --no-fund

ENTRYPOINT ["node", "scripts/run-overture-places-refresh-stage.mjs"]
