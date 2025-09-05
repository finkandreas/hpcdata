FROM docker.io/ubuntu:24.04 as builder

ARG GOLANG_VERSION=1.24

RUN apt update && \
    apt install -y software-properties-common && \
    add-apt-repository ppa:longsleep/golang-backports && \
    apt update && \
    apt install -y golang-$GOLANG_VERSION && \
    ln -s /usr/lib/go-$GOLANG_VERSION/bin/go /usr/bin/go

COPY . /sources
WORKDIR /sources
RUN go build

FROM docker.io/ubuntu:24.04
RUN apt update && apt upgrade -y && env DEBIAN_FRONTEND=noninteractive apt install -y  tzdata ca-certificates && rm -Rf /var/lib/apt/lists/*

COPY --from=builder /sources/hpcdata /usr/local/bin
