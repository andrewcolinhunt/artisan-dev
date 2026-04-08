# --- Build stage: resolve all dependencies from pixi.lock ---
FROM ghcr.io/prefix-dev/pixi:0.66.0 AS build
WORKDIR /app
COPY pyproject.toml pixi.lock ./
COPY src/ src/
RUN pixi install --locked

# --- Runtime stage: slim image with resolved environment ---
FROM ubuntu:24.04
WORKDIR /app
COPY --from=build /app/.pixi/envs/default /app/.pixi/envs/default
COPY --from=build /app/src /app/src
ENV PATH="/app/.pixi/envs/default/bin:$PATH"
ENV CONDA_PREFIX="/app/.pixi/envs/default"
ENV PREFECT_LOGGING_LEVEL="CRITICAL"
