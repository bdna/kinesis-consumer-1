package dynamostreams

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/bdna/kinesis-consumer-1/internal"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams/dynamodbstreamsiface"
	consumer "github.com/harlow/kinesis-consumer"
)

// DynamoStreamsConsumer wraps the interaction with the DynamoStream
type DynamoStreamsConsumer struct {
	client                   dynamodbstreamsiface.DynamoDBStreamsAPI
	initialShardIteratorType string
	logger                   consumer.Logger
}

// NewDynamoStreamsConsumer returns a pointer to a DynamoStreamsConsumer. If no
// options are passed the DynamoStreamsConsumer is configured with default settings.
// Use any of the DynamoStreamOption functions to override any of the default settings.
// For example you can pass your own client that implements dynamodbstreamsiface.DynamoDBStreamsAPI
// by calling NewDynamoStreamsConsumer(WithDynamoStreamsClient(<your client))
func NewDynamoStreamsConsumer(opts ...Option) (*DynamoStreamsConsumer, error) {
	d := &DynamoStreamsConsumer{
		initialShardIteratorType: dynamodbstreams.ShardIteratorTypeLatest,
	}

	for _, opt := range opts {
		opt(d)
	}

	if d.client == nil {
		sess, err := session.NewSession(aws.NewConfig())
		if err != nil {
			return &DynamoStreamsConsumer{}, err
		}
		d.client = dynamodbstreams.New(sess)
	}

	if d.logger == nil {
		d.logger = &internal.NoopLogger{
			Logger: log.New(ioutil.Discard, "", log.LstdFlags),
		}
	}

	return d, nil
}

// Scan launches a goroutine to process each of the shards in the stream. The callback
// function is passed to each of the goroutines and called for each message pulled from
// the stream. The seqNum parameter is optional and if a SequenceNumber is passed then
// Scan will start reading from that point on the stream. If you just pass an empty
// string to seqNum then Scan will read from the DynamoStreamsConsumers initialShardIteratorType.
// If you haven't overidden the initialShardIteratorType when calling NewDynamoStreamsConsumer
// it will default to SHARD_ITERATOR_TYPE LATEST
func (d *DynamoStreamsConsumer) Scan(ctx context.Context, arn string, seqNum string, fn func(*dynamodbstreams.Record) error) error {
	errc := make(chan error, 1)
	shardc := make(chan *dynamodbstreams.Shard, 1)
	broker := newDynamoStreamsBroker(d.client, arn, shardc)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go broker.start(ctx)

	go func() {
		<-ctx.Done()
		close(shardc)
	}()

	for shard := range shardc {
		go func(shardID string) {
			if err := d.scanShard(ctx, arn, shardID, seqNum, fn); err != nil {
				select {
				case errc <- fmt.Errorf("shard %s has error: %v", shardID, err):
					cancel()
				}
			}
		}(*shard.ShardId)
	}
	close(errc)
	return <-errc
}

// getStreamArn takes a table name and returns the arn of its associated dynamodbstream
func (d *DynamoStreamsConsumer) GetStreamArn(tableName string) (string, error) {
	return d.getStreamArn(tableName)
}

func (d *DynamoStreamsConsumer) getStreamArn(tableName string) (string, error) {
	stream, err := d.client.ListStreams(&dynamodbstreams.ListStreamsInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return ``, fmt.Errorf("couldn't get arn for stream %q: %s", tableName, err)
	}

	// We should only get one stream back for our stream name which means we should
	// be able to just pick the StreamArn for the the first item in the Streams slice
	return *stream.Streams[0].StreamArn, nil
}

// getShardIterator returns the shardIterator for a stream. If a sequence number is passed
// it will return the shardIterator for that point in the stream. Otherwise it returns
// the shardIterator for the DynamoStreamsConsumers initialShardIteratorType
func (d *DynamoStreamsConsumer) getShardIterator(arn, shardID, seqNum string) (string, error) {
	input := &dynamodbstreams.GetShardIteratorInput{
		ShardId:   aws.String(shardID),
		StreamArn: aws.String(arn),
	}

	if seqNum != "" {
		input.ShardIteratorType = aws.String(dynamodbstreams.ShardIteratorTypeAfterSequenceNumber)
		input.SequenceNumber = aws.String(seqNum)
	} else {
		input.ShardIteratorType = aws.String(d.initialShardIteratorType)
	}

	res, err := d.client.GetShardIterator(input)
	if err != nil {
		return ``, fmt.Errorf(`get shard iteratror error: %s`, err)
	}
	return *res.ShardIterator, nil
}

// ScanShard loops over the records in a specific shard and calls the callback
// function passed to it on each record.
//
// The seqNum parameter is optional and if a SequenceNumber is passed then Scan
// will start reading from that point on the stream. If you just pass an empty0 string
// to seqNum then Scan will read from the DynamoStreamsConsumers initialShardIteratorType.
// unless the default checkpoint was overriden when calling NewDynamoStreamsConsumer.
func (d *DynamoStreamsConsumer) scanShard(ctx context.Context, arn, shardID, seqNum string, fn func(*dynamodbstreams.Record) error) error {
	shardIterator, err := d.getShardIterator(arn, shardID, seqNum)
	if err != nil {
		return fmt.Errorf("get shard iterator error: %v", err)
	}

	d.logger.Log("[START]\t", map[string]interface{}{
		"arn":                  arn,
		"shard_id":             shardID,
		"last_sequence_number": seqNum,
	})
	defer func() {
		d.logger.Log("[STOP]\t", map[string]interface{}{
			"arn":                  arn,
			"shard_id":             shardID,
			"last_sequence_number": seqNum,
		})
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			resp, err := d.client.GetRecords(&dynamodbstreams.GetRecordsInput{
				ShardIterator: aws.String(shardIterator),
			})
			if err != nil {
				return err
			}

			for _, r := range resp.Records {
				select {
				case <-ctx.Done():
					return nil
				default:
					err := fn(r)
					if err != nil {
						return err
					}
					seqNum = *r.Dynamodb.SequenceNumber
				}
			}

			if shardClosed(resp.NextShardIterator, &shardIterator) {
				d.logger.Log("[CLOSED]\t", shardID)
				return nil
			}
			shardIterator = *resp.NextShardIterator
		}
	}
}

// shardClosed returns a boolean value that represents whether or not the
// shard has been closed
func shardClosed(nextShardIterator, currentShardIterator *string) bool {
	return nextShardIterator == nil || currentShardIterator == nextShardIterator
}
