package saramaproducer

import (
	"context"
	"github.com/Ficoto/kafka-interface/message"
	"github.com/Ficoto/kafka-interface/producer"
	"github.com/Shopify/sarama"
)

type Producer struct {
	addrs         []string
	config        *sarama.Config
	msgType       message.MsgType
	asyncProducer sarama.AsyncProducer
	syncProducer  sarama.SyncProducer
}

func New(setters ...Setter) (producer.Producer, error) {
	var p = new(Producer)
	p.config = sarama.NewConfig()
	for _, setter := range setters {
		setter(p)
	}
	var err error
	p.asyncProducer, err = sarama.NewAsyncProducer(p.addrs, p.config)
	if err != nil {
		return nil, err
	}
	config := *p.config
	config.Producer.Return.Successes = true
	p.syncProducer, err = sarama.NewSyncProducer(p.addrs, &config)
	return p, err
}

func (p *Producer) generateMsg(ctx context.Context, topic string, key string, msg any) (*sarama.ProducerMessage, error) {
	v, err := message.MsgToByte(ctx, msg, p.msgType)
	if err != nil {
		return nil, err
	}
	return &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(v),
	}, nil
}

func (p *Producer) AsyncSendMsg(ctx context.Context, topic string, key string, msg any) error {
	m, err := p.generateMsg(ctx, topic, key, msg)
	if err != nil {
		return err
	}
	p.asyncProducer.Input() <- m
	return nil
}

func (p *Producer) SyncSendMsg(ctx context.Context, topic string, key string, msg any) error {
	m, err := p.generateMsg(ctx, topic, key, msg)
	if err != nil {
		return err
	}
	_, _, err = p.syncProducer.SendMessage(m)
	return err
}
