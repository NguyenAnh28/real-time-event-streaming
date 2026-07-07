FROM golang:1.25-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /out/producer ./cmd/producer
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/detector ./cmd/detector
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/api ./cmd/api
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/benchmark ./cmd/benchmark

FROM alpine:3.20 AS runtime

RUN addgroup -S app && adduser -S app -G app
USER app

COPY --from=build /out/producer /usr/local/bin/producer
COPY --from=build /out/detector /usr/local/bin/detector
COPY --from=build /out/api /usr/local/bin/api
COPY --from=build /out/benchmark /usr/local/bin/benchmark

EXPOSE 8080

CMD ["/usr/local/bin/api"]

