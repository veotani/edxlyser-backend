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

// NewSequentialTopicKafka returns kafka.Service connected to
// SequentialEvents kafka topic
func NewSequentialTopicKafka(host string, port int) Service {
	return newConnection(host, port, "SequentialEvents")
}

// NewBookmarksTopicKafka returns kafka.Service connected to
// BookmarksEvents kafka topic
func NewBookmarksTopicKafka(host string, port int) Service {
	return newConnection(host, port, "BookmarksEvents")
}

// NewLinksTopicKafka returns kafka.Service connected to
// LinksEvents kafka topic
func NewLinksTopicKafka(host string, port int) Service {
	return newConnection(host, port, "LinksEvents")
}
