# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e
FROM golang:1.26.8-bookworm@sha256:a688600ca24f8a4d3ca77f95b0dd40704a9fc787c826660eb7ba0b641b8b175d AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG VERSION=dev
ARG REVISION=unknown
ARG BUILT_AT=unknown
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath \
    -ldflags="-s -w -X github.com/sauhard74/mem-jev/internal/buildinfo.version=${VERSION} -X github.com/sauhard74/mem-jev/internal/buildinfo.commit=${REVISION} -X github.com/sauhard74/mem-jev/internal/buildinfo.builtAt=${BUILT_AT}" \
    -o /out/memjev-api ./cmd/api
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath \
    -ldflags="-s -w -X github.com/sauhard74/mem-jev/internal/buildinfo.version=${VERSION} -X github.com/sauhard74/mem-jev/internal/buildinfo.commit=${REVISION} -X github.com/sauhard74/mem-jev/internal/buildinfo.builtAt=${BUILT_AT}" \
    -o /out/memjev-worker ./cmd/worker
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath \
    -ldflags="-s -w -X github.com/sauhard74/mem-jev/internal/buildinfo.version=${VERSION} -X github.com/sauhard74/mem-jev/internal/buildinfo.commit=${REVISION} -X github.com/sauhard74/mem-jev/internal/buildinfo.builtAt=${BUILT_AT}" \
    -o /out/memjev-admin ./cmd/admin

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
ARG VERSION=dev
ARG REVISION=unknown
LABEL org.opencontainers.image.source="https://github.com/sauhard74/mem-jev" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.memjev.schema.max-version="14"
COPY --from=build --chown=65532:65532 /out/memjev-api /memjev-api
COPY --from=build --chown=65532:65532 /out/memjev-worker /memjev-worker
COPY --from=build --chown=65532:65532 /out/memjev-admin /memjev-admin
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/memjev-api"]
