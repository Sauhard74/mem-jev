# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e
FROM golang:1.23.3-bookworm@sha256:59b8183301af6dc358c9258d7b2ab0ee1a9363618552334fb3b160d454cbda72 AS build
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

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
ARG VERSION=dev
ARG REVISION=unknown
LABEL org.opencontainers.image.source="https://github.com/sauhard74/mem-jev" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}"
COPY --from=build --chown=65532:65532 /out/memjev-api /memjev-api
COPY --from=build --chown=65532:65532 /out/memjev-worker /memjev-worker
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/memjev-api"]
