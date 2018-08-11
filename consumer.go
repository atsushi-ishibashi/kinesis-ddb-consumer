package consumer

import (
	"fmt"
	"time"
)

type Consumer interface {
	SetMaxWait(i int) error
	SetChannelCap(i int) error
	GetChannel() <-chan Record
}

type Record struct {
	ArrivalTimestamp time.Time
	Data             []byte
}

type kinesisConsumer struct {
	app, stream, table string
	shards             []string
	maxWait            int
	recordCh           chan Record

	ddbsvc     *ddbClient
	kinesissvc *kinesisClient
}

func New(appName, streamName, tableName string) (Consumer, error) {
	kc := newKinesisClient(streamName)

	ss, err := kc.getShards()
	if err != nil {
		return nil, err
	}

	dc := newDdbClient(appName, streamName, tableName)

	go dc.runSave()

	return &kinesisConsumer{
		app:        appName,
		stream:     streamName,
		table:      tableName,
		shards:     ss,
		maxWait:    20,
		recordCh:   make(chan Record),
		ddbsvc:     dc,
		kinesissvc: kc,
	}, nil
}

func (c *kinesisConsumer) SetMaxWait(i int) error {
	if i < 0 {
		return ErrInvalidMaxWait
	}
	c.maxWait = i
	return nil
}

func (c *kinesisConsumer) SetChannelCap(i int) error {
	if i < 0 {
		return ErrInvalidChannelCap
	}
	c.recordCh = make(chan Record, i)
	return nil
}

func (c *kinesisConsumer) GetChannel() <-chan Record {
	for _, v := range c.shards {
		go c.readLoop(v)
	}
	return c.recordCh
}

func (c *kinesisConsumer) readLoop(shardID string) {
	seqNum, err := c.ddbsvc.getSequenceNumber(shardID)
	if err != nil {
		// TODO:
		fmt.Println(err)
	}
	iterator, err := c.kinesissvc.getShardIterator(shardID, seqNum)
	if err != nil {
		// TODO:
		fmt.Println(err)
	}

	sleepTime := 0
	for {
		resp, err := c.kinesissvc.getRecords(iterator)
		if err != nil {
			// TODO:
			fmt.Println(err)
			seqNum, _ = c.ddbsvc.getSequenceNumber(shardID)
			iterator, _ = c.kinesissvc.getShardIterator(shardID, seqNum)
			continue
		}
		for _, v := range resp.Records {
			rec := Record{
				Data: v.Data,
			}
			if v.ApproximateArrivalTimestamp != nil {
				rec.ArrivalTimestamp = *v.ApproximateArrivalTimestamp
			}
			c.recordCh <- rec
			c.ddbsvc.setSequenceNumber(shardID, *v.SequenceNumber)
		}

		if resp.NextShardIterator != nil {
			iterator = *resp.NextShardIterator
		}

		if len(resp.Records) > 0 {
			sleepTime = 0
		} else {
			if sleepTime == 0 {
				sleepTime = 1
			} else if sleepTime < c.maxWait {
				sleepTime = sleepTime * 2
			}
		}

		time.Sleep(time.Duration(sleepTime) * time.Second)
	}
}
