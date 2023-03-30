package kafkagoproducer

import (
	"context"
	"math/rand"
	"testing"
	"time"
)

func TestProducer_SyncSendMsg(t *testing.T) {
	p := New(SetAddr("localhost:9092"), SetBatchTimeout(time.Millisecond*50))
	var a struct {
		A string
	}
	a.A = "test msg2"
	err := p.SyncSendMsg(context.Background(), "kafka-interface-test", "test", a)
	if err != nil {
		t.Fatal(err)
	}
}

func generateList() []int {
	var res []int
	rand.Seed(int64(time.Now().Nanosecond()))
	max := rand.Intn(100000)
	for i := 0; i != max; i++ {
		res = append(res, rand.Intn(1000))
	}
	return res
}

func TestSetLogger(t *testing.T) {
	for i := 0; i != 3; i++ {
		t.Log(generateList())
	}
}

func TestSetCompletion(t *testing.T) {
	var a = make(map[int][]int)
	for i := 0; i != 5; i++ {
		rand.Seed(int64(time.Now().Nanosecond()))
		key := rand.Intn(100000)
		a[key] = generateList()
	}
	for k, v := range a {
		a[k] = generateList()
		t.Log(k, len(v), len(a[k]))
	}
	for k, _ := range a {
		a[k] = []int{}
		t.Log(k)
	}
}
