package consumer

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

var (
	kinesisconn kinesisiface.KinesisAPI
)

func init() {
	kinesisconn = kinesis.New(session.New(aws.NewConfig().WithRegion("ap-northeast-1")))
}

func getShards(stream string) ([]string, error) {
	shards := make([]string, 0)

	input := &kinesis.ListShardsInput{
		StreamName: aws.String(stream),
	}
	resp, err := kinesisconn.ListShards(input)
	if err != nil {
		return shards, err
	}
	for _, v := range resp.Shards {
		shards = append(shards, *v.ShardId)
	}
	return shards, nil
}

func getShardIterator(stream, shardID, seqNum string) (string, error) {
	input := &kinesis.GetShardIteratorInput{
		ShardId:           aws.String(shardID),
		ShardIteratorType: aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber),
		StreamName:        aws.String(stream),
	}
	if seqNum == "" {
		input.ShardIteratorType = aws.String(kinesis.ShardIteratorTypeTrimHorizon)
	} else {
		input.StartingSequenceNumber = aws.String(seqNum)
	}

	resp, err := kinesisconn.GetShardIterator(input)
	if err != nil {
		return "", err
	}
	return *resp.ShardIterator, nil
}

func getRecords(iterator string) (*kinesis.GetRecordsOutput, error) {
	input := &kinesis.GetRecordsInput{
		ShardIterator: aws.String(iterator),
	}
	return kinesisconn.GetRecords(input)
}
