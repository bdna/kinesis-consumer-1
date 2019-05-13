# Go DynamoDBStreams Consumer

## Overview

The DynamoDBStreams consumer provides an interface for consuming records from a DynamoDBStream by exposing a `Scan` function that consumes records from all shards on the DynmoDBStream and calls the callback function passed to it on each record consumed from the DynamoDBStream.

```go
Scan(ctx context.Context, arn string, seqNum string, fn func(*dynamodbstreams.Record), error) error
```

## Example

```go
import (
    "log"
    "fmt"
    "github.com/aws/aws-sdk-go/service/dynamodbstreams"

    dynamoconsumer "github.com/harlow/kinesis-consumer/dynamostreams"
)

func main() {
    c, err := dynamoconsumer.NewDynamoStreamsConsumer()
    if err != nil {
        log.Fatal("couldn't create dynamostreams consumer: %v", err)
    }


    arn, err := c.GetStreamArn("<table name>")
    if err != nil {
        log.Fatal("couldn't get arn for stream: %v", err)
    }


    // Passing an empty sequence number means we will start consuming for the
    // DynamoStreamConsumers initialShardIteratorType. If we passed a sequence
    // number in here then we could start consuming from a specifc point on the
    // stream.
    //
    // Passing a SequenceNumber to Scan will also set the ShardIteratorType to
    // AFTER_SEQUENCE_NUMBER, overriding the initialShardIteratorType or any
    // shardIteratorType passed as an option the NewDynamoStreamsConsumer.
    err = c.Scan(context.TODO(), arn, `` func(r *dynamodbstreams.Record) error {
        fmt.Println(r)
    })
    if err != nil {
        log.Fatal("scan error: %v", err)
    }
}
```

## Options

The DynamoDBStreams consumer allows the following optional overrides to be passed to it.

### DynamoDBStreams Client

Option for passing your own DynamoDBStreams client to the consumer

```go
import (
    "log"

    "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/dynamodbstreams"
    
    dynamoconsumer "github.com/harlow/kinesis-consumer/dynamostreams"
)

sess := session.Must(session.NewSession(aws.NewConfig()))
client := dynamodbstreams.New(sess)

c, err := dynamostreams.NewDynamoStreamsConsumer(dynamostreams.WithClient(client))
if err != nil {
    log.Fatal("failed to create consumer: %v", err)
}
```

### Logging

You can pass your own logger to the DynamoStreamsConsumer provided it implementes the logger interface

```go
import (
    "log"
    "os"

    dynamoconsumer "github.com/harlow/kinesis-consumer/dynamostreams"
)

type Logger interface {
    Log(v ...interface{})
}
```

```go
type yourLogger struct {
    *log.Logger
}

func (l *yourLogger) Log(v ...interface{}) {
    l.Print(v)
}

logger := &yourLogger{log.New(os.Stdout, "consumer-example", log.LstdFlags)}

c, err := dynamostreams.NewDynamoStreamsConsumer(dynamostreams.WithLogger(logger))
if err != nil {
    log.Fatal("failed to create consumer: %v", err)
}
```

### ShardIteratorType

The default DynamoStreamsConsumer client will use `LATEST` as the ShardIteratorType when reading from the stream. This means that it will start reading from record that was most recently pushed to the stream. You can change this value by passing in a ShardIteratorType when instating your DynamoDBStreamsClient.

```go
import (
    "log"
)

c, err := dynamostreams.NewDynamoStreamsConsumer(dynamostreams.WithShardIteratorType(`TRIM_HORIZON`))
if err != nil {
    log.Fatal("failed to create consumer: %v", err)
}
```

### Reading from a specific record

If you want to read from a specific point in the stream then you pass a sequence number when calling `Scan`. Passing a sequence number to the `Scan` method will override the ShardIteratorType to be `AFTER_SEQUENCE_NUMBER` and the consumer will start reading from the first record in the stream after this sequence number. A limitation of this implementation is that the client consuming from the stream has to keep track of the sequence number for each record they consume if they want to be able to start consuming from the same point again in the event that that the connection goes down at some point.

```go
c, err := dynamostreams.NewDynamoStreamsConsumer()
if err != nil {
    log.Fatal("failed to create consumer: %v", err)
}

sequenceNumber := `1234` 

err = c.Scan(context.TODO(), arn, sequenceNumber func(r *dynamodbstreams.Record) error {
    // r will be the first record in the stream with a sequence number after 1234
    fmt.Println(r)
})
if err != nil {
    log.Fatal("scan error: %v", err)
}
```