FROM golang

WORKDIR /go/src/kafka-log-processor
COPY . .

RUN go get -d -v ./...
RUN go install -v ./...
RUN ls

CMD ["go", "run", "cmd/video_parser/main.go"]