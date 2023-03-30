package kafkagoconsumer

import (
	"context"
	"github.com/Ficoto/go-gp"
	"github.com/Ficoto/kafka-interface/consumer"
	"github.com/Ficoto/kafka-interface/logger"
	"github.com/Ficoto/kafka-interface/message"
	"github.com/segmentio/kafka-go"
	"sync"
)

type Consumer struct {
	config     kafka.ReaderConfig
	topics     []string
	logger     logger.LogWriter
	msgType    message.MsgType
	ctxPool    sync.Pool
	handlerMap map[string]map[string]consumer.RealHandler
	reader     *kafka.Reader
	ctx        context.Context
	cancel     context.CancelFunc
}

func New(setters ...Setter) consumer.Consumer {
	var c = new(Consumer)
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
		m, _ := msg.(kafka.Message)
		ctx, err := consumer.GenerateCTXByByte(context.Background(), m.Value, c.msgType)
		if err != nil {
			return err
		}
		return handle(ctx)
	}
}

func (c *Consumer) generateCallback(callback func(msg any, err error)) func(msg any, err error) {
	return func(msg any, err error) {
		m, _ := msg.(kafka.Message)
		if c.reader != nil {
			err := c.reader.CommitMessages(context.Background(), m)
			if err != nil {
				c.logger.Printf("CommitMessages fail,err:%v", err)
			}
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
	if len(c.topics) == 1 && len(c.config.GroupID) == 0 {
		c.config.Topic = c.topics[0]
	} else {
		c.config.GroupTopics = c.topics
	}
	c.reader = kafka.NewReader(c.config)
	c.ctx, c.cancel = context.WithCancel(context.Background())
	go func() {
		for {
			select {
			case <-c.ctx.Done():
				return
			default:
				msg, err := c.reader.FetchMessage(c.ctx)
				if err != nil {
					c.logger.Printf("FetchMessage fail,err:%v", err)
					continue
				}
				if handle, ok := c.handlerMap[msg.Topic][string(msg.Key)]; ok {
					err = handle.GP.GoWithTask(gp.Task{
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
					err := c.reader.CommitMessages(context.Background(), msg)
					if err != nil {
						c.logger.Printf("CommitMessages fail,err:%v", err)
					}
				}
			}
		}
	}()
}

func (c *Consumer) Close() {
	c.cancel()
	if err := c.reader.Close(); err != nil {
		c.logger.Printf("reader close fail,err:%v", err)
	}
	for _, khMap := range c.handlerMap {
		for _, handle := range khMap {
			handle.GP.Close()
		}
	}
}
