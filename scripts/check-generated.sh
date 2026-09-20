#!/bin/sh
set -eu

go run github.com/bufbuild/buf/cmd/buf@v1.47.2 generate
git diff --exit-code -- gen
