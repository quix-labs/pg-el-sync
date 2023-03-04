# Imports a GO alpine image
FROM golang:1.19-alpine

# Sets environment variables necessary for building
ENV GO111MODULE=on \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64
# Install upx for building
RUN apk add upx

# Creates the application's directory
RUN mkdir -p /src

# Sets the work directory to application's folder
WORKDIR /src

# Copy files into application's folder
COPY ../src .

# Install the dependencies
RUN go mod download

# Installing reflex
RUN go install github.com/cespare/reflex@latest

# Running a reflex job for hot-reloading
CMD ["reflex", "-c", "./reflex.conf"]