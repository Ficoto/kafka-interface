package kafkagoconsumer

import (
	"fmt"
	"github.com/Ficoto/kafka-interface/consumer"
	"github.com/segmentio/kafka-go"
	"testing"
	"time"
)

type testLogger struct {
}

func (tl *testLogger) Println(v ...any) {
	fmt.Println(v...)
}

func (tl *testLogger) Printf(s string, v ...any) {
	fmt.Printf(s, v...)
}

func TestNew(t *testing.T) {
	var h = func(ctx consumer.Context) error {
		var a struct {
			A string
		}
		err := ctx.BindJson(&a)
		if err != nil {
			return err
		}
		t.Logf("this is handle,msg:%v", a)
		return nil
	}
	c := New(SetBrokers("localhost:9092"), SetGroupID("group-id2"), SetLogger(&testLogger{}))
	c.AddHandler(consumer.Handler{
		Topic:  "kafka-interface-test",
		Key:    "test",
		Handle: h,
	}, consumer.Handler{
		Topic:  "kafka-test",
		Key:    "test",
		Handle: h,
	})
	c.Run()
	time.Sleep(time.Hour)
	c.Close()
}

func TestNew2(t *testing.T) {
	var h = func(ctx consumer.Context) error {
		var a struct {
			A string
		}
		err := ctx.BindJson(&a)
		if err != nil {
			return err
		}
		t.Logf("this is handle,msg:%v", a)
		return nil
	}
	c := New(SetBrokers("localhost:9092"), SetGroupID("group-id2"), SetLogger(&testLogger{}), SetStartOffset(kafka.LastOffset))
	c.AddHandler(consumer.Handler{
		Topic:  "kafka-interface-test",
		Key:    "test",
		Handle: h,
	}, consumer.Handler{
		Topic:  "kafka-test",
		Key:    "test",
		Handle: h,
	})
	c.Run()
	time.Sleep(time.Hour)
	c.Close()
}
