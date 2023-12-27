



# Imports a GO alpine image
FROM golang:1.21.5-alpine3.19 as builder

# Sets environment variables necessary for building
ENV GO111MODULE=on \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

ENV USER=nonroot
ENV UID=10001

# Create passwd empty file
RUN echo "$USER:x:$UID:$UID::/nonexistent:/sbin/nologin" > /etc/passwd
RUN echo "$USER:x:$UID:" > /etc/group

# Fetch dump-init
RUN wget -O /usr/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.5/dumb-init_1.2.5_x86_64
RUN chmod +x /usr/bin/dumb-init

# Creates the application's directory
RUN mkdir -p /src
WORKDIR $GOPATH/src/alancolant/go_pg_es_sync
COPY src .

# Fetch dependencies.
RUN go mod download
RUN go mod verify

RUN go build -ldflags="-w -s" -o /go/bin/pgsync


# Final image
FROM scratch

COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group
COPY --from=builder /go/bin/pgsync /go/bin/pgsync
COPY --from=builder /usr/bin/dumb-init /usr/bin/dumb-init

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
USER nonroot:nonroot
CMD ["/go/bin/pgsync","listen"]
