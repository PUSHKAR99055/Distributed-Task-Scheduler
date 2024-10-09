# Use an official Go runtime as a parent image
FROM golang:1.21

# Install Protobuf compiler and Go-related packages
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    golang-go \
    git

# Install the protoc-gen-go and protoc-gen-go-grpc plugins
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Set the working directory in the container
WORKDIR /app

# Copy the go.mod and go.sum files first to leverage Docker cache
COPY go.mod go.sum ./

# Download all dependencies
RUN go mod download

# Copy the local package files to the container's workspace.
COPY pkg/ ./pkg/
COPY cmd/coordinator/main.go .

#RUN chmod 755 ./pkg/grpcapi/build.sh
RUN ./pkg/grpcapi/build.sh

# Build the coordinator application
RUN go build -o coordinator main.go

# Run the coordinator when the container launches
CMD ["./coordinator", "--coordinator_port=:8080"]
