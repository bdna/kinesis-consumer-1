package dynamostreams

import (
	"github.com/aws/aws-sdk-go/service/dynamodbstreams/dynamodbstreamsiface"
	consumer "github.com/harlow/kinesis-consumer"
)

// DynamoStreamOption is used to override default values when creating a new
// DynamoStreamsConsumer
type Option func(*DynamoStreamsConsumer)

// WithLogger overrides the default logger
func WithLogger(logger consumer.Logger) Option {
	return func(d *DynamoStreamsConsumer) {
		d.logger = logger
	}
}

// WithClient overrides the default client
func WithClient(client dynamodbstreamsiface.DynamoDBStreamsAPI) Option {
	return func(d *DynamoStreamsConsumer) {
		d.client = client
	}
}

// ShardIteratorType overrides the starting point for the consumer
func WithShardIteratorType(t string) Option {
	return func(d *DynamoStreamsConsumer) {
		d.initialShardIteratorType = t
	}
}
