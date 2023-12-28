



# Imports a GO alpine image
FROM golang:1.21.5-alpine3.19 as builder

# Sets environment variables necessary for building
ENV GO111MODULE=on \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

ENV USER=nonroot
ENV UID=10001

RUN mkdir -p /.output/etc
RUN mkdir -p /.output/bin

# Create passwd empty file
RUN echo "$USER:x:$UID:$UID::/nonexistent:/sbin/nologin" > /.output/etc/passwd
RUN echo "$USER:x:$UID:" > /.output/etc/group

# Fetch dump-init
RUN wget -O /.output/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.5/dumb-init_1.2.5_x86_64
RUN chmod +x /.output/bin/dumb-init

# Creates the application's directory
WORKDIR $GOPATH/src/alancolant/go_pg_es_sync
COPY src .

# Fetch dependencies.
RUN go mod download
RUN go mod verify

RUN go build -ldflags="-w -s" -o /.output/bin/pgsync


# Final image
FROM alpine:3.19.0
COPY --from=builder /.output /

ENTRYPOINT ["/bin/dumb-init", "--"]
USER nonroot:nonroot
CMD ["/bin/pgsync","listen"]
