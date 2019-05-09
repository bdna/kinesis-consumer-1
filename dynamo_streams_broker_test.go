package consumer

import (
	"reflect"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

const (
	mockArn      = `1234`
	mockArnError = `uh oh`
)

var (
	mockShards = []*dynamodbstreams.Shard{
		&dynamodbstreams.Shard{},
	}
)

func TestNewDynamoStreamsBroker(t *testing.T) {
	const arn = `1234`
	var (
		client = &mockDynamoClient{}
		shardc = make(chan *dynamodbstreams.Shard)
	)
	broker := newDynamoStreamsBroker(client, arn, shardc)

	if broker.client == nil {
		t.Errorf(`expected broker.client to not be nil but it was`)
	}
	if !reflect.DeepEqual(broker.client, client) {
		t.Errorf(`expected broker to be of type %T: got %T`, client, broker.client)
	}

	if broker.streamArn != arn {
		t.Errorf(`expected streamArn to be %q: got %q`, arn, broker.streamArn)
	}

	if broker.shardc == nil {
		t.Errorf(`expected broker.shardc to not be nil but it was`)
	}
	if !reflect.DeepEqual(broker.shardc, shardc) {
		t.Errorf(`expected broker.shardc to be of type %T: got %T`, shardc, broker.shardc)
	}

	if !reflect.DeepEqual(broker.shardMu, &sync.Mutex{}) {
		t.Errorf(`expectedd broker.shardMu to be of type %T: got %T`, broker.shardMu, &sync.Mutex{})
	}

	if broker.shards == nil {
		t.Errorf(`expected broker.Shards to not be nil but it was`)
	}
	expShardsType := map[string]*dynamodbstreams.Shard{}
	if !reflect.DeepEqual(broker.shards, expShardsType) {
		t.Errorf(`expeccted broker.shards to be of type %T: got %T`, expShardsType, broker.shardMu)
	}
}

func TestDynamoStreamsBroker_listShards(t *testing.T) {
	testCases := []struct {
		desc      string
		broker    *dynamoStreamsBroker
		shouldErr bool

		expShards []*dynamodbstreams.Shard
	}{
		{
			desc: `calling listShards with a dynamoStreamsBroker that shouldn't fail`,
			broker: &dynamoStreamsBroker{
				client: &mockDynamoClient{
					shards: mockShards,
				},
				streamArn: mockArn,
			},
			shouldErr: false,

			expShards: mockShards,
		},
		{
			desc: `Calling listShards with a dynamoStreamsBroker that should fail`,
			broker: &dynamoStreamsBroker{
				client:    &mockDynamoClient{},
				streamArn: mockArnError,
			},
			shouldErr: true,

			expShards: []*dynamodbstreams.Shard{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			shards, err := tc.broker.listShards()

			if tc.shouldErr {
				if err == nil {
					t.Errorf(`expected error to not be nil got: %v`, err)
				}
			} else {
				if err != nil {
					t.Errorf(`expected error to be nil got: %q`, err)
				}
			}

			if !reflect.DeepEqual(shards, tc.expShards) {
				t.Errorf(`expected shards to be %v: got %v`, tc.expShards, shards)
			}
		})
	}
}
