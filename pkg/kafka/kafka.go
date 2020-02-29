package kafka

import (
	"context"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	_ "github.com/segmentio/kafka-go/gzip" // gzip is a package for log decompression
)

// Service is an interface for kafka
type Service struct {
	topic  string
	reader *kafka.Reader
}

// NextMessage returns next message value via kafka.Service
func (s Service) NextMessage() ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	m, err := s.reader.ReadMessage(ctx)
	if err != nil {
		return nil, err
	}
	return m.Value, nil
}

func newConnection(host string, port int, topic string) Service {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{host + ":" + strconv.Itoa(port)},
		Topic:     topic,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e7, // 10MB
	})

	return Service{
		topic:  topic,
		reader: reader,
	}
}
