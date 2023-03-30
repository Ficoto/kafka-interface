package kafkagoproducer

import (
	"context"
	"github.com/Ficoto/kafka-interface/logger"
	"github.com/Ficoto/kafka-interface/message"
	"github.com/Ficoto/kafka-interface/producer"
	"github.com/segmentio/kafka-go"
	"net"
	"sync"
	"time"
)

type writer struct {
	AsyncWriter *kafka.Writer
	SyncWriter  *kafka.Writer
}

type Producer struct {
	addr                   net.Addr
	balancer               kafka.Balancer
	topicList              []string
	twMap                  sync.Map
	maxAttempts            int
	writeBackoffMin        time.Duration
	writeBackoffMax        time.Duration
	batchSize              int
	batchBytes             int64
	batchTimeout           time.Duration
	readTimeout            time.Duration
	writeTimeout           time.Duration
	requiredAcks           kafka.RequiredAcks
	completion             func(messages []kafka.Message, err error)
	compression            kafka.Compression
	logger                 logger.LogWriter
	transport              kafka.RoundTripper
	allowAutoTopicCreation bool
	msgType                message.MsgType
}

func New(setters ...Setter) producer.Producer {
	var p = new(Producer)
	for _, setter := range setters {
		setter(p)
	}
	if p.balancer == nil {
		p.balancer = &kafka.Hash{}
	}
	for _, topic := range p.topicList {
		if _, ok := p.twMap.Load(topic); ok {
			continue
		}
		generateWriter(p, topic)
	}
	return p
}

func generateWriter(p *Producer, topic string) *writer {
	w := &writer{
		AsyncWriter: &kafka.Writer{
			Addr:                   p.addr,
			Topic:                  topic,
			Balancer:               p.balancer,
			MaxAttempts:            p.maxAttempts,
			WriteBackoffMin:        p.writeBackoffMin,
			WriteBackoffMax:        p.writeBackoffMax,
			BatchSize:              p.batchSize,
			BatchBytes:             p.batchBytes,
			BatchTimeout:           p.batchTimeout,
			ReadTimeout:            p.readTimeout,
			WriteTimeout:           p.writeTimeout,
			RequiredAcks:           p.requiredAcks,
			Completion:             p.completion,
			Compression:            p.compression,
			Logger:                 p.logger,
			ErrorLogger:            p.logger,
			Transport:              p.transport,
			AllowAutoTopicCreation: p.allowAutoTopicCreation,
			Async:                  true,
		},
		SyncWriter: &kafka.Writer{
			Addr:                   p.addr,
			Topic:                  topic,
			Balancer:               p.balancer,
			MaxAttempts:            p.maxAttempts,
			WriteBackoffMin:        p.writeBackoffMin,
			WriteBackoffMax:        p.writeBackoffMax,
			BatchSize:              p.batchSize,
			BatchBytes:             p.batchBytes,
			BatchTimeout:           p.batchTimeout,
			ReadTimeout:            p.readTimeout,
			WriteTimeout:           p.writeTimeout,
			RequiredAcks:           p.requiredAcks,
			Completion:             p.completion,
			Compression:            p.compression,
			Logger:                 p.logger,
			ErrorLogger:            p.logger,
			Transport:              p.transport,
			AllowAutoTopicCreation: p.allowAutoTopicCreation,
		},
	}
	p.twMap.Store(topic, w)
	return w
}

func (p *Producer) generateMsg(ctx context.Context, key string, msg any) (kafka.Message, error) {
	v, err := message.MsgToByte(ctx, msg, p.msgType)
	if err != nil {
		return kafka.Message{}, err
	}
	return kafka.Message{
		Key:   []byte(key),
		Value: v,
	}, nil
}

func (p *Producer) AsyncSendMsg(ctx context.Context, topic string, key string, msg any) error {
	w, ok := p.twMap.Load(topic)
	if !ok {
		w = generateWriter(p, topic)
	}
	write, _ := w.(*writer)
	m, err := p.generateMsg(ctx, key, msg)
	if err != nil {
		return err
	}
	err = write.AsyncWriter.WriteMessages(ctx, m)
	return err
}

func (p *Producer) SyncSendMsg(ctx context.Context, topic string, key string, msg any) error {
	w, ok := p.twMap.Load(topic)
	if !ok {
		w = generateWriter(p, topic)
	}
	write, _ := w.(*writer)
	m, err := p.generateMsg(ctx, key, msg)
	if err != nil {
		return err
	}
	err = write.SyncWriter.WriteMessages(ctx, m)
	return err
}
