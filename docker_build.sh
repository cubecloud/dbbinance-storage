#!/bin/bash
# Build dbbinance-image. TOKEN env var must hold a GitHub PAT with repo
# read access (the Dockerfile clones the repo over HTTPS during build).
# Local override on a build host: keep TOKEN in a per-host .env (gitignored).
if [[ -z "${TOKEN}" ]]; then
    echo "Error: TOKEN env var must be set (GitHub PAT for repo clone)" >&2
    exit 1
fi
sudo docker build --no-cache --progress=plain --build-arg TOKEN="${TOKEN}" -t dbbinance-image .
