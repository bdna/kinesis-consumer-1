package dynamostreams

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams/dynamodbstreamsiface"
)

// dynamoStreamsBroker caches a local list of shards that we're already processing
// and polls the stream every 30 seconds for new shards
type dynamoStreamsBroker struct {
	client    dynamodbstreamsiface.DynamoDBStreamsAPI
	streamArn string

	shardc  chan *dynamodbstreams.Shard
	shardMu *sync.Mutex
	shards  map[string]*dynamodbstreams.Shard
}

// newDynamoStreamsBroker returns a pointer to an initialisedd dynamoStreamsBroker
func newDynamoStreamsBroker(c dynamodbstreamsiface.DynamoDBStreamsAPI, arn string, shardc chan *dynamodbstreams.Shard) *dynamoStreamsBroker {
	return &dynamoStreamsBroker{
		client:    c,
		streamArn: arn,
		shardc:    shardc,
		shards:    make(map[string]*dynamodbstreams.Shard),
		shardMu:   &sync.Mutex{},
	}
}

// start polls the dynamodbstream for new shards every 30 seconds
func (d *dynamoStreamsBroker) start(ctx context.Context) {
	d.findNewShards()

	ticker := time.NewTicker(30 * time.Second)

	// Note: while ticker is a rather naive approach to this problem,
	// it actually simplies a few things. i.e. If we miss a new shard while
	// AWS is resharding we'll pick it up max 30 seconds later.

	// It might be worth refactoring this flow to allow the consumer to
	// to notify the broker when a shard is closed. However, shards don't
	// necessarily close at the same time, so we could potentially get a
	// thundering heard of notifications from the consumer.
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
			d.findNewShards()
		}
	}
}

// findNewShards pulls the list of shards from the dynamodbstreams API and
// stores them in the brokers local cache if they don't already exist in it.
func (d *dynamoStreamsBroker) findNewShards() {
	d.shardMu.Lock()
	defer d.shardMu.Unlock()

	shards, err := d.listShards()
	if err != nil {
		log.Println(`uh oh`)
		return
	}

	for _, shard := range shards {
		if _, ok := d.shards[*shard.ShardId]; ok {
			continue
		}
		d.shards[*shard.ShardId] = shard
		d.shardc <- shard
	}
}

// listShards pulls shards for a dynanmodbstream from the dynamodbstreams API
func (d *dynamoStreamsBroker) listShards() ([]*dynamodbstreams.Shard, error) {
	shards := []*dynamodbstreams.Shard{}
	input := &dynamodbstreams.DescribeStreamInput{
		StreamArn: aws.String(d.streamArn),
	}

	resp, err := d.client.DescribeStream(input)
	if err != nil {
		return []*dynamodbstreams.Shard{}, err
	}
	shards = append(shards, resp.StreamDescription.Shards...)
	return shards, nil
}
