.PHONY: build run-invoicing run-metering run-billing run-fakemetronome run-fakestripe run-controlplane tidy vet web-install web-dev clean

build:
	go build -o bin/invoicing ./cmd/invoicing
	go build -o bin/metering ./cmd/metering
	go build -o bin/billing ./cmd/billing
	go build -o bin/fakemetronome ./cmd/fakemetronome
	go build -o bin/fakestripe ./cmd/fakestripe
	go build -o bin/controlplane ./cmd/controlplane

run-invoicing:
	go run ./cmd/invoicing

run-metering:
	go run ./cmd/metering

run-billing:
	go run ./cmd/billing

run-fakemetronome:
	go run ./cmd/fakemetronome

run-fakestripe:
	go run ./cmd/fakestripe

run-controlplane:
	go run ./cmd/controlplane

tidy:
	go mod tidy

vet:
	go vet ./...

web-install:
	cd web && npm install

web-dev:
	cd web && npm run dev

clean:
	rm -rf bin web/dist web/node_modules
