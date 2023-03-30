package saramaconsumer

import (
	"context"
	"github.com/Ficoto/go-gp"
	"github.com/Ficoto/kafka-interface/consumer"
	"github.com/Ficoto/kafka-interface/logger"
	"github.com/Ficoto/kafka-interface/message"
	"github.com/Shopify/sarama"
	"sync"
)

type Consumer struct {
	config     *sarama.Config
	topics     []string
	logger     logger.LogWriter
	msgType    message.MsgType
	ctxPool    sync.Pool
	handlerMap map[string]map[string]consumer.RealHandler
	ctx        context.Context
	cancel     context.CancelFunc
	brokers    []string
	groupID    string
	cg         sarama.ConsumerGroup
	cgs        sarama.ConsumerGroupSession
}

func New(setters ...Setter) consumer.Consumer {
	var c = new(Consumer)
	c.config = sarama.NewConfig()
	for _, setter := range setters {
		setter(c)
	}
	c.ctxPool.New = func() any {
		return consumer.Context{}
	}
	if c.logger == nil {
		c.logger = logger.NopLogger{}
	}
	return c
}

func (c *Consumer) generateHandle(handle func(ctx consumer.Context) error) func(msg any) error {
	return func(msg any) error {
		m, _ := msg.(*sarama.ConsumerMessage)
		ctx, err := consumer.GenerateCTXByByte(context.Background(), m.Value, c.msgType)
		if err != nil {
			return err
		}
		return handle(ctx)
	}
}

func (c *Consumer) generateCallback(callback func(msg any, err error)) func(msg any, err error) {
	return func(msg any, err error) {
		m, _ := msg.(*sarama.ConsumerMessage)
		if c.cgs != nil {
			c.cgs.MarkMessage(m, "")
		}
		if callback == nil {
			return
		}
		callback(m, err)
	}
}

func (c *Consumer) AddHandler(handlers ...consumer.Handler) {
	if c.handlerMap == nil {
		c.handlerMap = make(map[string]map[string]consumer.RealHandler)
	}
	for _, handler := range handlers {
		if _, ok := c.handlerMap[handler.Topic]; !ok {
			c.topics = append(c.topics, handler.Topic)
			c.handlerMap[handler.Topic] = make(map[string]consumer.RealHandler)
		}
		var rh consumer.RealHandler
		rh.Handle = c.generateHandle(handler.Handle)
		rh.Callback = c.generateCallback(handler.Callback)
		rh.IsRetry = handler.IsRetry
		if rh.IsRetry == nil && handler.RetryTimes != 0 {
			rh.IsRetry = consumer.IsRetryByFailTimes(handler.RetryTimes)
		}
		rh.GP = gp.New(gp.SetLogger(c.logger), gp.SetMaxPoolSize(handler.PoolSize))
		rh.GP.Run()
		c.handlerMap[handler.Topic][handler.Key] = rh
	}
}

func (c *Consumer) Run() {
	if len(c.topics) == 0 {
		return
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	var err error
	c.cg, err = sarama.NewConsumerGroup(c.brokers, c.groupID, c.config)
	if err != nil {
		c.logger.Printf("Error creating consumer group client: %v", err)
		return
	}
	go func() {
		for {
			if err := c.cg.Consume(c.ctx, c.topics, c); err != nil {
				c.logger.Printf("Error from consumer: %v", err)
				return
			}
			if c.ctx.Err() != nil {
				return
			}
		}
	}()
}

func (c *Consumer) Close() {
	c.cancel()
	if err := c.cg.Close(); err != nil {
		c.logger.Printf("consumer client close fail,err:%v", err)
	}
	for _, khMap := range c.handlerMap {
		for _, handle := range khMap {
			handle.GP.Close()
		}
	}
}

func (c *Consumer) Setup(session sarama.ConsumerGroupSession) error {
	c.cgs = session
	return nil
}

func (c *Consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	c.cgs = nil
	return nil
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg := <-claim.Messages():
			if handle, ok := c.handlerMap[msg.Topic][string(msg.Key)]; ok {
				err := handle.GP.GoWithTask(gp.Task{
					Message:  msg,
					Handler:  handle.Handle,
					Callback: handle.Callback,
					IsRetry:  handle.IsRetry,
				})
				if err != nil {
					c.logger.Printf("GoWithMessage fail,err:%v", err)
					continue
				}
			} else {
				session.MarkMessage(msg, "")
			}
		case <-session.Context().Done():
			return nil
		}
	}
}
