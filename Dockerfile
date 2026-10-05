FROM golang:1.27.1-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY main.go ./
COPY internal/ ./internal/
COPY gen/ ./gen/
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/gate .

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 gate \
    && useradd --uid 10001 --gid gate --create-home gate \
    && mkdir /workspace \
    && chown gate:gate /workspace

COPY --from=build /out/gate /usr/local/bin/gate

USER gate
WORKDIR /workspace
EXPOSE 8080

ENTRYPOINT ["gate", "-listen", "0.0.0.0:8080"]
