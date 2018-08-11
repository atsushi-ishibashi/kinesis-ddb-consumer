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

type kinesisClient struct {
	kinesisconn kinesisiface.KinesisAPI
	stream      string
}

func newKinesisClient(stream string) *kinesisClient {
	return &kinesisClient{
		kinesisconn: kinesis.New(session.New(aws.NewConfig().WithRegion("ap-northeast-1"))),
		stream:      stream,
	}
}

func (c *kinesisClient) getShards() ([]string, error) {
	shards := make([]string, 0)

	input := &kinesis.ListShardsInput{
		StreamName: aws.String(c.stream),
	}
	resp, err := c.kinesisconn.ListShards(input)
	if err != nil {
		return shards, err
	}
	for _, v := range resp.Shards {
		shards = append(shards, *v.ShardId)
	}
	return shards, nil
}

func (c *kinesisClient) getShardIterator(shardID, seqNum string) (string, error) {
	input := &kinesis.GetShardIteratorInput{
		ShardId:           aws.String(shardID),
		ShardIteratorType: aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber),
		StreamName:        aws.String(c.stream),
	}
	if seqNum == "" {
		input.ShardIteratorType = aws.String(kinesis.ShardIteratorTypeTrimHorizon)
	} else {
		input.StartingSequenceNumber = aws.String(seqNum)
	}

	resp, err := c.kinesisconn.GetShardIterator(input)
	if err != nil {
		return "", err
	}
	return *resp.ShardIterator, nil
}

func (c *kinesisClient) getRecords(iterator string) (*kinesis.GetRecordsOutput, error) {
	input := &kinesis.GetRecordsInput{
		ShardIterator: aws.String(iterator),
	}
	return c.kinesisconn.GetRecords(input)
}
