package consumer

import (
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type ddbClient struct {
	dynamoconn         dynamodbiface.DynamoDBAPI
	app, stream, table string
	seqmu              sync.RWMutex
	shardSeqMap        map[string]string
}

func newDdbClient(app, stream, table string) *ddbClient {
	return &ddbClient{
		dynamoconn:  dynamodb.New(session.New(aws.NewConfig().WithRegion("ap-northeast-1"))),
		app:         app,
		stream:      stream,
		table:       table,
		shardSeqMap: make(map[string]string),
	}
}

type ddbRecord struct {
	AppName        string `dynamodbav:"AppName"`
	StreamShard    string `dynamodbav:"StreamShard"`
	SequenceNumber string `dynamodbav:"SequenceNumber"`
}

func (c *ddbClient) getSequenceNumber(shard string) (string, error) {
	input := &dynamodb.GetItemInput{
		TableName:      aws.String(c.table),
		ConsistentRead: aws.Bool(true),
		Key: map[string]*dynamodb.AttributeValue{
			"AppName": &dynamodb.AttributeValue{
				S: aws.String(c.app),
			},
			"StreamShard": &dynamodb.AttributeValue{
				S: aws.String(fmt.Sprintf("%s_%s", c.stream, shard)),
			},
		},
	}
	resp, err := c.dynamoconn.GetItem(input)
	if err != nil {
		return "", err
	}

	var dr ddbRecord
	if err := dynamodbattribute.UnmarshalMap(resp.Item, &dr); err != nil {
		return "", err
	}
	return dr.SequenceNumber, nil
}

func (c *ddbClient) setSequenceNumber(shard string, seqNum string) {
	c.seqmu.Lock()
	defer c.seqmu.Unlock()

	exist := false
	for k := range c.shardSeqMap {
		if k == shard {
			exist = true
			c.shardSeqMap[k] = seqNum
		}
	}

	if !exist {
		c.shardSeqMap[shard] = seqNum
	}
}

func (c *ddbClient) runSave() {
	ticker := time.NewTicker(10 * time.Second)

	for _ = range ticker.C {
		c.saveSequenceNumber()
	}
}

func (c *ddbClient) saveSequenceNumber() {
	c.seqmu.RLock()
	defer c.seqmu.RUnlock()

	for shard, seqnum := range c.shardSeqMap {
		if err := c.putSequenceNumber(shard, seqnum); err != nil {
			fmt.Println(err)
		}
	}
}

func (c *ddbClient) putSequenceNumber(shard, seqNum string) error {
	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeNames: map[string]*string{
			"#SN": aws.String("SequenceNumber"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":sn": {
				S: aws.String(seqNum),
			},
		},
		Key: map[string]*dynamodb.AttributeValue{
			"AppName": {
				S: aws.String(c.app),
			},
			"StreamShard": {
				S: aws.String(fmt.Sprintf("%s_%s", c.stream, shard)),
			},
		},
		ConditionExpression: aws.String("attribute_not_exists(#SN) OR #SN <= :sn"),
		TableName:           aws.String(c.table),
		UpdateExpression:    aws.String("SET #SN = :sn"),
	}

	_, err := c.dynamoconn.UpdateItem(input)
	return err
}
