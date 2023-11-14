all: 
	env GOOS=linux GOARCH=amd64 go build -o citus-amd64 . && \
	env GOOS=linux GOARCH=arm64 go build -o citus-arm64 . && \
	git add . && git commit -m "update" && git push
