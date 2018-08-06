# kinesis-ddb-consumer
[![GoDoc][1]][2]

[1]: https://godoc.org/github.com/atsushi-ishibashi/kinesis-ddb-consumer?status.svg
[2]: https://godoc.org/github.com/atsushi-ishibashi/kinesis-ddb-consumer

KinesisStream Client for golang which manage sequence number in DynamoDB.

## Overview
The package will abstract the shards of the kinesis stream so you can read data via single channel.

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

### requirements
#### IAM Policy
```
{
  "Version": "2012-10-17",
  "Statement": [{
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetItem",
        "dynamodb:UpdateItem"
      ],
      "Resource": [
        "arn:aws:dynamodb:<region>:<account>:table/<table>"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "kinesis:GetShardIterator",
        "kinesis:GetRecords"
      ],
      "Resource": [
        "arn:aws:kinesis:<region>:<account>:stream/<stream>"
      ]
    },
    {
      "Effect": "Allow",
      "Action": "kinesis:ListShards",
      "Resource": "*"
    }
  ]
}
```

### TODO
- Graceful shatdown
- Context
