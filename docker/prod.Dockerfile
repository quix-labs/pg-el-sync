# Imports a GO alpine image
FROM golang:1.19-alpine as build

# Sets environment variables necessary for building
ENV GO111MODULE=on \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

# Creates the application's directory
RUN mkdir -p /src


WORKDIR /src
COPY ../src .

RUN go build -o ./pgsync

FROM golang:1.19-alpine

WORKDIR /app

RUN apk add --no-cache supervisor
RUN apk add --update coreutils && rm -rf /var/cache/apk/*
RUN mkdir -p /var/log/supervisor

COPY --from=build /src/pgsync /usr/bin/pgsync
RUN chmod u+x /usr/bin/pgsync

ADD ../supervisord.conf /etc/supervisord.conf

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisord.conf"]
