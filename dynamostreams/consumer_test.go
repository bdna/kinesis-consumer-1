package dynamostreams

import (
	"context"
	"errors"
	"io/ioutil"
	"log"
	"reflect"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams/dynamodbstreamsiface"
	"github.com/bdna/kinesis-consumer-1/internal"
	consumer "github.com/harlow/kinesis-consumer"
)

const (
	validTableName   = "foo"
	invalidTableName = "bad"
	validArn         = "1234"

	validShardID       = "1"
	invalidShardID     = "invalid"
	validShardIterator = "1"

	validEventID   = "1"
	invalidEventID = "0"

	checkpointArn     = "checkpoint arn"
	checkpointShardID = "checkpoint shardID"
	checkpointSeqNum  = `2`
)

var dynamoRecord = &dynamodbstreams.Record{
	EventID: aws.String(validEventID),
	Dynamodb: &dynamodbstreams.StreamRecord{
		SequenceNumber: aws.String("1234"),
	},
}

type mockCheckpoint struct{}

func (m *mockCheckpoint) Set(a string, b string, c string) error { return nil }

func (m *mockCheckpoint) Get(arn string, shardID string) (string, error) {
	if arn == checkpointArn && shardID == checkpointShardID {
		return checkpointSeqNum, nil
	}
	return "", nil
}

type mockLogger struct{}

func (m *mockLogger) Log(...interface{}) {}

type mockDynamoClient struct {
	dynamodbstreamsiface.DynamoDBStreamsAPI
	shards []*dynamodbstreams.Shard
	mutex  *sync.Mutex
}

func (m *mockDynamoClient) DescribeStream(i *dynamodbstreams.DescribeStreamInput) (*dynamodbstreams.DescribeStreamOutput, error) {
	if *i.StreamArn == mockArnError {
		return &dynamodbstreams.DescribeStreamOutput{}, errors.New("an error")
	}
	output := &dynamodbstreams.DescribeStreamOutput{
		StreamDescription: &dynamodbstreams.StreamDescription{
			StreamArn: i.StreamArn,
			Shards:    m.shards,
		},
	}
	return output, nil
}

func (m *mockDynamoClient) ListStreams(i *dynamodbstreams.ListStreamsInput) (*dynamodbstreams.ListStreamsOutput, error) {
	if *i.TableName == validTableName {
		output := &dynamodbstreams.ListStreamsOutput{
			LastEvaluatedStreamArn: aws.String(""),
			Streams: []*dynamodbstreams.Stream{
				&dynamodbstreams.Stream{
					StreamArn:   aws.String(validArn),
					StreamLabel: aws.String("test"),
					TableName:   aws.String(validTableName),
				},
			},
		}
		return output, nil
	} else if *i.TableName == invalidTableName {
		return &dynamodbstreams.ListStreamsOutput{}, errors.New("an error")
	}

	return &dynamodbstreams.ListStreamsOutput{}, errors.New("unexpected test case")
}

func (m *mockDynamoClient) GetShardIterator(i *dynamodbstreams.GetShardIteratorInput) (*dynamodbstreams.GetShardIteratorOutput, error) {
	if i.SequenceNumber != nil {
		if *i.SequenceNumber != `` {
			output := &dynamodbstreams.GetShardIteratorOutput{
				ShardIterator: aws.String(validShardIterator),
			}
			return output, nil
		}
	}

	if *i.ShardId == validShardID {
		output := &dynamodbstreams.GetShardIteratorOutput{
			ShardIterator: aws.String(validShardIterator),
		}
		return output, nil
	} else if *i.ShardId == invalidShardID {
		return &dynamodbstreams.GetShardIteratorOutput{}, errors.New("uh oh")
	}

	return &dynamodbstreams.GetShardIteratorOutput{}, errors.New("unexpected test case")
}

func (m *mockDynamoClient) GetRecords(i *dynamodbstreams.GetRecordsInput) (*dynamodbstreams.GetRecordsOutput, error) {
	if *i.ShardIterator == validShardIterator {
		output := &dynamodbstreams.GetRecordsOutput{
			NextShardIterator: aws.String("1"),
			Records:           []*dynamodbstreams.Record{dynamoRecord},
		}
		return output, nil
	}

	return &dynamodbstreams.GetRecordsOutput{}, errors.New("unexpected test case")
}

func TestNewDynamoStreamsConsumer(t *testing.T) {
	testCases := []struct {
		desc string

		checkpointOpt    Option
		loggerOpt        Option
		shardIteratorOpt Option
		clientOpts       Option

		expLogger                consumer.Logger
		expInitShardIteratorType string
		expClient                dynamodbstreamsiface.DynamoDBStreamsAPI

		shouldErr bool
	}{
		{
			desc: "When I pass no options to NewDynamoStreamsConsumer, then the default values will be assinged",

			checkpointOpt:    func(*DynamoStreamsConsumer) {},
			loggerOpt:        func(*DynamoStreamsConsumer) {},
			shardIteratorOpt: func(*DynamoStreamsConsumer) {},
			clientOpts:       func(*DynamoStreamsConsumer) {},

			expLogger: &internal.NoopLogger{
				Logger: log.New(ioutil.Discard, "", log.LstdFlags),
			},
			expInitShardIteratorType: dynamodbstreams.ShardIteratorTypeLatest,
			expClient:                &dynamodbstreams.DynamoDBStreams{},

			shouldErr: false,
		},
		{
			desc: "When I pass options to NewDynamoStreamsConsumer, then the options will be applied",

			loggerOpt:        WithLogger(&mockLogger{}),
			shardIteratorOpt: WithShardIteratorType("foo"),
			clientOpts:       WithClient(&mockDynamoClient{}),

			expLogger:                &mockLogger{},
			expInitShardIteratorType: "foo",
			expClient:                &mockDynamoClient{},

			shouldErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {

			d, err := NewDynamoStreamsConsumer(tc.loggerOpt, tc.shardIteratorOpt, tc.clientOpts)

			if tc.shouldErr {
				if err == nil {
					t.Errorf("expected error to not be nil but it was")
				}
			} else {
				if err != nil {
					t.Errorf("expected error to be nil: got %v", err)
				}
			}

			if reflect.TypeOf(d.logger) != reflect.TypeOf(tc.expLogger) {
				t.Errorf("expected d.logger to be of type %T: got %T", tc.expLogger, d.logger)
			}

			if d.initialShardIteratorType != tc.expInitShardIteratorType {
				t.Errorf("expected d.initialShardIteratorType to be %q: got %q", tc.expInitShardIteratorType, d.initialShardIteratorType)
			}

			if reflect.TypeOf(d.client) != reflect.TypeOf(tc.expClient) {
				t.Errorf("expected d.client to be of type %T: got %T", tc.expClient, d.client)
			}
		})
	}
}

func TestDynamoStreamsConsumer_getStreamArn(t *testing.T) {
	testCases := []struct {
		desc      string
		consumer  *DynamoStreamsConsumer
		tableName string

		shouldErr bool
		expArn    string
	}{
		{
			desc: "Calling getStreamArn will return an arn and no error",
			consumer: &DynamoStreamsConsumer{
				client: &mockDynamoClient{},
			},
			tableName: validTableName,

			shouldErr: false,
			expArn:    validArn,
		},
		{
			desc: "Calling getStreamArn should return an error and an empty string",
			consumer: &DynamoStreamsConsumer{
				client: &mockDynamoClient{},
			},
			tableName: invalidTableName,

			shouldErr: true,
			expArn:    ``,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			actual, err := tc.consumer.getStreamArn(tc.tableName)
			if tc.shouldErr {
				if err == nil {
					t.Errorf("expected error to not be nil but it was")
				}
			} else {
				if err != nil {
					t.Errorf("expected error to be nil: got %v", err)
				}
			}

			if actual != tc.expArn {
				t.Errorf("expected arn to be %q: got %q", tc.expArn, actual)
			}
		})
	}
}

func TestDynamoStreamsConsumer_getShardIterator(t *testing.T) {
	testCases := []struct {
		desc        string
		consumer    *DynamoStreamsConsumer
		shardID     string
		seqNum      string
		shouldErr   bool
		expIterator string
	}{
		{
			desc: "Calling getShardIterator should return a ShardIterator and no error",
			consumer: &DynamoStreamsConsumer{
				client:                   &mockDynamoClient{},
				initialShardIteratorType: "blah",
			},
			shardID:     validShardID,
			seqNum:      "",
			shouldErr:   false,
			expIterator: validShardIterator,
		},
		{
			desc: "Calling getShardIterator should return an empty string and an error",
			consumer: &DynamoStreamsConsumer{
				client: &mockDynamoClient{},
			},
			shardID:     invalidShardID,
			seqNum:      "",
			shouldErr:   true,
			expIterator: "",
		},
	}

	const (
		arn = "1"
	)

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {

			actual, err := tc.consumer.getShardIterator(arn, tc.shardID, tc.seqNum)
			if tc.shouldErr {
				if err == nil {
					t.Errorf("expected error to not be nil but it was")
				}
			} else {
				if err != nil {
					t.Errorf("expected error to be nil: got %v", err)
				}
			}

			if actual != tc.expIterator {
				t.Errorf("expected shard iterator to be %q: got %q", tc.expIterator, actual)
			}
		})
	}
}

func TestDynamoStreamsConsumer_scanShard(t *testing.T) {
	testCases := []struct {
		desc      string
		consumer  *DynamoStreamsConsumer
		shardID   string
		seqNum    string
		shouldErr bool
	}{
		{
			desc: "Calling scanShard with a shardID that will not cause and error",
			consumer: &DynamoStreamsConsumer{
				client:                   &mockDynamoClient{},
				logger:                   &mockLogger{},
				initialShardIteratorType: "blah",
			},
			shardID:   validShardID,
			seqNum:    "",
			shouldErr: false,
		},
		{
			desc: "Calling scanShard with a shardID that will cause an error",
			consumer: &DynamoStreamsConsumer{
				client:                   &mockDynamoClient{},
				logger:                   &mockLogger{},
				initialShardIteratorType: "blah",
			},
			shardID:   "This will end badly",
			seqNum:    "",
			shouldErr: true,
		},
		{
			desc: "Calling scanShard with a shardID that will not cause and error and passing a sequence number",
			consumer: &DynamoStreamsConsumer{
				client:                   &mockDynamoClient{},
				logger:                   &mockLogger{},
				initialShardIteratorType: "blah",
			},
			shardID:   validShardID,
			seqNum:    "",
			shouldErr: false,
		},
	}

	const (
		arn = "1"
	)

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {

			ctx, cancel := context.WithCancel(context.Background())

			callback := func(r *dynamodbstreams.Record) error {
				defer cancel()

				if *r.EventID == validEventID {
					return nil
				} else if *r.EventID == invalidEventID {
					return errors.New("an error")
				}
				return errors.New("unexpected error case")
			}

			err := tc.consumer.scanShard(ctx, arn, tc.shardID, tc.seqNum, callback)
			if tc.shouldErr {
				if err == nil {
					t.Errorf(`expected error to not be nil but it was`)
				}
			} else {
				if err != nil {
					t.Errorf(`expected error to be nil: got %s`, err)
				}
			}
		})
	}
}

func TestDynamoStreamsConsumer_Scan(t *testing.T) {
	testCases := []struct {
		desc      string
		consumer  *DynamoStreamsConsumer
		seqNum    string
		shouldErr bool
	}{
		{
			desc: "Calling Scan with a stream containing a valid shard",
			consumer: &DynamoStreamsConsumer{
				client: &mockDynamoClient{
					shards: []*dynamodbstreams.Shard{
						&dynamodbstreams.Shard{
							ParentShardId:       aws.String("0"),
							ShardId:             aws.String(validShardID),
							SequenceNumberRange: &dynamodbstreams.SequenceNumberRange{},
						},
					},
				},
				initialShardIteratorType: "foo",
				logger:                   &mockLogger{},
			},
			seqNum:    "",
			shouldErr: false,
		},
		{
			desc: "Calling Scan with a stream containing an invalid shard",
			consumer: &DynamoStreamsConsumer{
				client: &mockDynamoClient{
					shards: []*dynamodbstreams.Shard{
						&dynamodbstreams.Shard{
							ParentShardId:       aws.String("0"),
							ShardId:             aws.String(invalidShardID),
							SequenceNumberRange: &dynamodbstreams.SequenceNumberRange{},
						},
					},
				},
				initialShardIteratorType: "foo",
				logger:                   &mockLogger{},
			},
			seqNum:    "",
			shouldErr: true,
		},
		{
			desc: "Calling Scan with a stream containing a valid shard and passing a sequence number",
			consumer: &DynamoStreamsConsumer{
				client: &mockDynamoClient{
					shards: []*dynamodbstreams.Shard{
						&dynamodbstreams.Shard{
							ParentShardId:       aws.String("0"),
							ShardId:             aws.String(validShardID),
							SequenceNumberRange: &dynamodbstreams.SequenceNumberRange{},
						},
					},
				},
				initialShardIteratorType: "foo",
				logger:                   &mockLogger{},
			},
			seqNum:    "1",
			shouldErr: false,
		},
	}

	const (
		arn = "123"
	)

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())

			callback := func(r *dynamodbstreams.Record) error {
				defer cancel()

				if *r.EventID == validEventID {
					return nil
				} else if *r.EventID == invalidEventID {
					return errors.New("an error")
				}
				return errors.New("unexpected error case")
			}

			err := tc.consumer.Scan(ctx, arn, tc.seqNum, callback)
			if tc.shouldErr {
				if err == nil {
					t.Errorf("expected error to not be nil but it was")
				}
			} else {
				if err != nil {
					t.Errorf("expected error to be nil: got %s", err)
				}
			}
		})
	}
}