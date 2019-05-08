package consumer

import "github.com/aws/aws-sdk-go/service/dynamodbstreams/dynamodbstreamsiface"

type DynamoStreamOption func(*DynamoStreamsConsumer)

func WithDynamoStreamsCheckpoint(checkpoint Checkpoint) DynamoStreamOption {
	return func(d *DynamoStreamsConsumer) {
		d.checkpoint = checkpoint
	}
}

// WithLogger overrides the default logger
func WithDynamoStreamsLogger(logger Logger) DynamoStreamOption {
	return func(d *DynamoStreamsConsumer) {
		d.logger = logger
	}
}

// WithClient overrides the default client
func WithDynamoStreamsClient(client dynamodbstreamsiface.DynamoDBStreamsAPI) DynamoStreamOption {
	return func(d *DynamoStreamsConsumer) {
		d.client = client
	}
}

// ShardIteratorType overrides the starting point for the consumer
func WithDynamoStreamsShardIteratorType(t string) DynamoStreamOption {
	return func(d *DynamoStreamsConsumer) {
		d.initialShardIteratorType = t
	}
}
