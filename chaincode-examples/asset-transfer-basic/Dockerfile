FROM golang:1.14-alpine AS build

WORKDIR /build
COPY . /build/
RUN GOPROXY=https://goproxy.cn CGO_ENABLED=0 go build -o /bin/asset-transfer-basic

FROM scratch
COPY --from=build /bin/asset-transfer-basic /bin/asset-transfer-basic
ENTRYPOINT ["/bin/asset-transfer-basic"]
