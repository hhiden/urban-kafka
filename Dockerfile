FROM centos:7
MAINTAINER Hugo Hiden <hhiden@redhat.com>
ARG BINARY=./urbansource

COPY ${BINARY} /opt/urbansource
ENTRYPOINT ["/opt/urbansource"]