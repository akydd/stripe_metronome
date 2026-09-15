# Shared build stage for all three Go services; the target is chosen with SERVICE.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download || true
COPY . .
ARG SERVICE
# go mod tidy resolves the Kafka client the first time, so the image builds even
# if go.sum hasn't been committed yet.
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/service ./cmd/${SERVICE}

# Minimal static runtime image.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/service /service
ENTRYPOINT ["/service"]
