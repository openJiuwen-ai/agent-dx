FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY main.go .
RUN CGO_ENABLED=0 go build -trimpath -o /echo .
FROM scratch
COPY --from=build /echo /echo
ENTRYPOINT ["/echo","sandbox"]
