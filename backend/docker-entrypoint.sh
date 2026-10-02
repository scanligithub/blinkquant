#!/bin/sh
set -eu

# HF Persistent Storage may mount /data as root-owned. Fix only the mount points;
# application-created files remain owned by the unprivileged runtime user.
mkdir -p /data /data/results
chown user:user /data /data/results

exec gosu user "$@"
