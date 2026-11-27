#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -z "${MODAL_PROXY_TOKEN_ID:-}" || -z "${MODAL_PROXY_TOKEN_SECRET:-}" ]]; then
  echo "error: MODAL_PROXY_TOKEN_ID / MODAL_PROXY_TOKEN_SECRET not set in this shell." >&2
  echo "Run from the terminal where you exported them (check: echo \$MODAL_PROXY_TOKEN_ID)," >&2
  echo "or mint a token at Modal dashboard -> Settings -> Proxy Auth Tokens." >&2
  exit 1
fi

cat > .env <<EOF
MODAL_PROXY_TOKEN_ID=$MODAL_PROXY_TOKEN_ID
MODAL_PROXY_TOKEN_SECRET=$MODAL_PROXY_TOKEN_SECRET
EOF
chmod 600 .env

echo "Wrote .env (token id: ${MODAL_PROXY_TOKEN_ID:0:6}..., gitignored, mode 600)"
