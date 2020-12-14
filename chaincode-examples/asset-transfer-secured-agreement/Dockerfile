FROM golang:1.14-alpine AS build

WORKDIR /build
COPY . /build/
RUN GOPROXY=https://goproxy.cn CGO_ENABLED=0 go build -o /bin/asset-transfer-secured-agreement

FROM scratch
COPY --from=build /bin/asset-transfer-secured-agreement /bin/asset-transfer-secured-agreement
ENTRYPOINT ["/bin/asset-transfer-secured-agreement"]
