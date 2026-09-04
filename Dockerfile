# syntax=docker/dockerfile:1

# ---- build ------------------------------------------------------------------
# Pinned to the lower bound of the CI matrix so the image is built with the
# oldest Go the project claims to support.
FROM golang:1.23-alpine AS build

# VERSION is stamped into main.version. Pass it from CI, e.g.
#   docker build --build-arg VERSION="$(git describe --tags --always --dirty)" .
# Left as "dev" it matches what a plain `go build` produces.
ARG VERSION=dev

WORKDIR /src

# go.mod alone is the whole dependency surface (the project is stdlib-only, so
# there is no go.sum). Copying it first still keeps this layer cached across
# source-only changes.
COPY go.mod ./
RUN go mod download

COPY . .

# CGO off so the binary is static and can run in a scratch-like base image.
# -trimpath keeps build-host paths out of the binary; -s -w drop the symbol
# table and DWARF, which the runtime does not need.
RUN CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/hedge-llm ./cmd/hedge-llm

# ---- runtime ----------------------------------------------------------------
# distroless/static has no shell and no package manager, so there is nothing to
# exec into and nothing to patch. :nonroot runs as uid 65532.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/hedge-llm /usr/local/bin/hedge-llm

# 8080 matches the default listen_addr. Override with HEDGE_LLM_LISTEN_ADDR.
EXPOSE 8080

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/hedge-llm"]
