FROM --platform=$BUILDPLATFORM golang:1.27.4-alpine AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown
WORKDIR /src
COPY go.mod go.sum ./
RUN apk add --no-cache git && go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" go build -trimpath \
    -ldflags="-s -w -X main.version=$VERSION -X main.commit=$COMMIT -X main.date=$BUILD_DATE" \
    -o /out/fortimetrics ./cmd/fortimetrics

FROM alpine:3.23
LABEL org.opencontainers.image.source="https://github.com/mihudec/fortimetrics"
RUN apk add --no-cache ca-certificates
COPY --from=build /out/fortimetrics /usr/local/bin/fortimetrics
USER 65532:65532
ENTRYPOINT ["fortimetrics"]
CMD ["-config", "/etc/fortimetrics/config.yml"]
