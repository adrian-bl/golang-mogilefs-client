default:
	go build -o mogile-client main.go

gofmt:
	find . -name "*.go" -exec gofmt -w {} \;
