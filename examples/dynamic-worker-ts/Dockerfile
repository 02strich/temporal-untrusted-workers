# @temporalio/core-bridge ships prebuilt native (napi-rs) binaries only for
# glibc targets (aarch64/x86_64-unknown-linux-gnu, among others) - no musl
# build exists, so this must be a glibc-based (Debian) image, never Alpine.
#FROM node:lts-bookworm-slim AS build
FROM ghcr.io/pnpm/pnpm:12 AS build
WORKDIR /app
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml tsconfig.json ./
#RUN npm ci
# COPY tsconfig.json ./
COPY src ./src
RUN pnpm run build
RUN pnpm run build:bundles

#FROM node:lts-bookworm-slim AS runtime
FROM ghcr.io/pnpm/pnpm:12 AS runtime
WORKDIR /app
ENV NODE_ENV=production
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml tsconfig.json ./
RUN pnpm ci
COPY --from=build /app/lib ./lib
COPY --from=build /app/bundles ./bundles
RUN apt-get update \
    && apt-get install -y ca-certificates \
    && rm -rf /var/lib/apt/lists/*
USER node
ENTRYPOINT ["pnpm", "run", "workers"]
