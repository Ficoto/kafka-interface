package saramaproducer

import (
	"context"
	"github.com/Shopify/sarama"
	"testing"
)

func TestProducer_SyncSendMsg(t *testing.T) {
	p, err := New(SetAddr("localhost:9092"), SetSaramaConfig(sarama.NewConfig()))
	if err != nil {
		t.Fatal(err)
	}
	var a struct {
		A string
	}
	a.A = "test msg3"
	err = p.SyncSendMsg(context.Background(), "kafka-interface-test", "test", a)
	if err != nil {
		t.Fatal(err)
	}
}
