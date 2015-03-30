default:
	go build -o mogile-client cmd/demo/main.go

gofmt:
	find . -name "*.go" -exec gofmt -w {} \;
