FROM golang:1.26.8 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/wfd ./cmd/wf ./examples/transfer/cmd/transfer-worker \
 && mkdir -p /out/data /out/ledger

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/wfd /out/wf /out/transfer-worker /
COPY --from=build --chown=65532:65532 /out/data /data
COPY --from=build --chown=65532:65532 /out/ledger /ledger
EXPOSE 7233
USER nonroot:nonroot
CMD ["/wfd", "--listen", ":7233", "--db", "/data/wf.db"]
