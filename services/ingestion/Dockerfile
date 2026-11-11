# Local deployment — Go API built without CGO.
FROM golang:1.26.10-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' -o /out/ingestion ./cmd/ingestion

FROM alpine:3.22
RUN apk add --no-cache ca-certificates && addgroup -S -g 10001 app && adduser -S -u 10001 -G app app
COPY --from=build /out/ingestion /usr/local/bin/ingestion
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/ingestion"]
