# --- Build stage: resolve all dependencies from pixi.lock ---
FROM ghcr.io/prefix-dev/pixi:0.66.0 AS build
WORKDIR /app
COPY pyproject.toml pixi.lock README.md ./
COPY src/ src/
# hatch-vcs uses setuptools-scm, which needs .git to determine the version.
# .git is excluded from the build context, so we provide a placeholder.
ENV SETUPTOOLS_SCM_PRETEND_VERSION="0.0.0.dev0"
RUN pixi install --locked

# --- Runtime stage: slim image with resolved environment ---
FROM ubuntu:24.04
# Distro CA bundle: Modal injects SSL_CERT_DIR=/etc/ssl/certs into every
# container, which preempts the pixi env's own cert.pem — without the
# bundle there, all TLS verification (S3/R2 uploads, presigned PUTs)
# fails with CERTIFICATE_VERIFY_FAILED.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app/.pixi/envs/default /app/.pixi/envs/default
COPY --from=build /app/src /app/src
ENV PATH="/app/.pixi/envs/default/bin:$PATH"
ENV CONDA_PREFIX="/app/.pixi/envs/default"
