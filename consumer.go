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
}

func New(appName, streamName, tableName string) (Consumer, error) {
	ss, err := getShards(streamName)
	if err != nil {
		return nil, err
	}

	go runSave(tableName)

	return &kinesisConsumer{
		app:      appName,
		stream:   streamName,
		table:    tableName,
		shards:   ss,
		maxWait:  20,
		recordCh: make(chan Record),
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
	seqNum, err := getSequenceNumber(c.app, c.stream, shardID, c.table)
	if err != nil {
		// TODO:
		fmt.Println(err)
	}
	iterator, err := getShardIterator(c.stream, shardID, seqNum)
	if err != nil {
		// TODO:
		fmt.Println(err)
	}

	sleepTime := 0
	for {
		resp, err := getRecords(iterator)
		if err != nil {
			// TODO:
			fmt.Println(err)
			seqNum, _ = getSequenceNumber(c.app, c.stream, shardID, c.table)
			iterator, _ = getShardIterator(c.stream, shardID, seqNum)
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
			setSequenceNumber(c.app, c.stream, shardID, *v.SequenceNumber)
		}

		if resp.NextShardIterator != nil {
			iterator = *resp.NextShardIterator
		}

		if resp.MillisBehindLatest != nil {
			if *resp.MillisBehindLatest == 0 && len(resp.Records) > 0 {
				sleepTime = 0
			} else {
				if sleepTime == 0 {
					sleepTime = 1
				} else if sleepTime < c.maxWait {
					sleepTime = sleepTime * 2
				}
			}
		}

		time.Sleep(time.Duration(sleepTime) * time.Second)
	}
}
