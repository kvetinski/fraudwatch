# Local deployment — Go API built without CGO from immutable base images.
FROM golang@sha256:5de7c99eb78ca5e515a4c340235d0955ec5964a5071c492c15503d7c267da473 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
ARG FRAUDWATCH_BUILD_REVISION=development
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath \
    -ldflags="-s -w -X main.buildRevision=${FRAUDWATCH_BUILD_REVISION}" \
    -o /out/ingestion ./cmd/ingestion

FROM alpine@sha256:49340b346798875b5592e5d420241f2f9cfba5f47e62ebb1446a1b34734e898f
ARG FRAUDWATCH_BUILD_REVISION=development
LABEL org.opencontainers.image.title="FraudWatch ingestion" \
      org.opencontainers.image.revision="${FRAUDWATCH_BUILD_REVISION}" \
      org.opencontainers.image.source="https://github.com/kvetinski/fraudwatch"
RUN addgroup -S -g 10001 app \
    && adduser -S -D -H -u 10001 -G app app
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/ingestion /usr/local/bin/ingestion
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/ingestion"]
