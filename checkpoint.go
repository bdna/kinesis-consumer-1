package consumer

// Checkpoint interface used track consumer progress in the stream
type Checkpoint interface {
	Get(streamName, shardID string) (string, error)
	Set(streamName, shardID, sequenceNumber string) error
}
