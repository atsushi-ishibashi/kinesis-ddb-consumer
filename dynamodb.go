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

var (
	dynamoconn dynamodbiface.DynamoDBAPI
	seqmu      sync.RWMutex
	seqMap     = make(map[*key]string)
)

type key struct {
	app    string
	stream string
	shard  string
}

func init() {
	dynamoconn = dynamodb.New(session.New(aws.NewConfig().WithRegion("ap-northeast-1")))
}

type ddbRecord struct {
	AppName        string `dynamodbav:"AppName"`
	StreamShard    string `dynamodbav:"StreamShard"`
	SequenceNumber string `dynamodbav:"SequenceNumber"`
}

func getSequenceNumber(app, stream, shard string, table string) (string, error) {
	input := &dynamodb.GetItemInput{
		TableName:      aws.String(table),
		ConsistentRead: aws.Bool(true),
		Key: map[string]*dynamodb.AttributeValue{
			"AppName": &dynamodb.AttributeValue{
				S: aws.String(app),
			},
			"StreamShard": &dynamodb.AttributeValue{
				S: aws.String(fmt.Sprintf("%s_%s", stream, shard)),
			},
		},
	}
	resp, err := dynamoconn.GetItem(input)
	if err != nil {
		return "", err
	}

	var dr ddbRecord
	if err := dynamodbattribute.UnmarshalMap(resp.Item, &dr); err != nil {
		return "", err
	}
	return dr.SequenceNumber, nil
}

func setSequenceNumber(app, stream, shard string, seqNum string) {
	seqmu.Lock()
	defer seqmu.Unlock()

	exist := false
	for k := range seqMap {
		if k.app == app && k.stream == stream && k.shard == shard {
			exist = true
			seqMap[k] = seqNum
		}
	}

	if !exist {
		k := &key{
			app:    app,
			stream: stream,
			shard:  shard,
		}
		seqMap[k] = seqNum
	}
}

func runSave(table string) {
	ticker := time.NewTicker(10 * time.Second)

	for _ = range ticker.C {
		saveSequenceNumber(table)
	}
}

func saveSequenceNumber(table string) {
	seqmu.RLock()
	defer seqmu.RUnlock()

	for k, v := range seqMap {
		if err := putSequenceNumber(k.app, k.stream, k.shard, v, table); err != nil {
			fmt.Println(err)
		}
	}
}

func putSequenceNumber(app, stream, shard, seqNum string, table string) error {
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
				S: aws.String(app),
			},
			"StreamShard": {
				S: aws.String(fmt.Sprintf("%s_%s", stream, shard)),
			},
		},
		ConditionExpression: aws.String("attribute_not_exists(#SN) OR #SN <= :sn"),
		TableName:           aws.String(table),
		UpdateExpression:    aws.String("SET #SN = :sn"),
	}

	_, err := dynamoconn.UpdateItem(input)
	return err
}
