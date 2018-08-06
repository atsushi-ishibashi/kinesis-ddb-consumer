# kinesis-ddb-consumer
KinesisStream Client for golang which manage sequence number in DynamoDB.

## Overview
The package will abstract shards of kinesis and you can read data via single channel.

This will guarantee the order of records in the same shard but won't guarantee the order across shards.

## Usage
```
import (
	consumer "github.com/atsushi-ishibashi/kinesis-ddb-consumer"
)

func main() {
	c, err := consumer.New("<AppName>", "<KinesisStreamName>", "<DynamoDB TableName>")
	if err != nil {
		log.Fatalln(err)
	}

	queue := c.GetChannel()
	for v := range queue {
		// do something
	}
}
```
