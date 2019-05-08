package consumer

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams/dynamodbstreamsiface"
)

type DynamoStreamsConsumer struct {
	client                   dynamodbstreamsiface.DynamoDBStreamsAPI
	initialShardIteratorType string
	logger                   Logger
	checkpoint               Checkpoint
}

func NewDynamoStreamsConsumer(opts ...DynamoStreamOption) (*DynamoStreamsConsumer, error) {
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
		d.logger = &noopLogger{
			logger: log.New(ioutil.Discard, "", log.LstdFlags),
		}
	}

	if d.checkpoint == nil {
		d.checkpoint = &noopCheckpoint{}
	}

	return d, nil
}

func (d *DynamoStreamsConsumer) Scan(ctx context.Context, arn string, fn func(*dynamodbstreams.Record) error) error {
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
			if err := d.scanShard(ctx, arn, shardID, fn); err != nil {
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

func (d *DynamoStreamsConsumer) scanShard(ctx context.Context, arn, shardID string, fn func(*dynamodbstreams.Record) error) error {
	lastSeqNum, err := d.checkpoint.Get(arn, shardID)
	if err != nil {
		return fmt.Errorf("get checkpoint error: %v", err)
	}

	shardIterator, err := d.getShardIterator(arn, shardID, lastSeqNum)
	if err != nil {
		return fmt.Errorf("get shard iterator error: %v", err)
	}

	d.logger.Log("[START]\t", map[string]interface{}{
		"arn":                  arn,
		"shard_id":             shardID,
		"last_sequence_number": lastSeqNum,
	})
	defer func() {
		d.logger.Log("[STOP]\t", map[string]interface{}{
			"arn":                  arn,
			"shard_id":             shardID,
			"last_sequence_number": lastSeqNum,
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
					lastSeqNum = *r.Dynamodb.SequenceNumber
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

func shardClosed(nextShardIterator, currentShardIterator *string) bool {
	return nextShardIterator == nil || currentShardIterator == nextShardIterator
}
