# use version that k4 is using
FROM golang:1.23.0-alpine

# set the working directory
WORKDIR /app

# copy mod files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# copy server files
COPY . .

# build the application
RUN go build -o server_example main.go

# expose server port
EXPOSE 8000

# command to run the server
CMD ["./server_example"]