package kafka

// NewVideoTopicKafka returns kafka.Service connected to
// VideoEvents kafka topic
func NewVideoTopicKafka(host string, port int) Service {
	return newConnection(host, port, "VideoEvents")
}

// NewProblemTopicKafka returns kafka.Service connected to
// TestEvents kafka topic
func NewProblemTopicKafka(host string, port int) Service {
	return newConnection(host, port, "TestEvents")
}
