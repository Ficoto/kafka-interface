package kafkagoproducer

import (
	"github.com/Ficoto/kafka-interface/logger"
	"github.com/Ficoto/kafka-interface/message"
	"github.com/segmentio/kafka-go"
	"time"
)

type Setter func(p *Producer)

func SetAddr(addr ...string) Setter {
	return func(p *Producer) {
		p.addr = kafka.TCP(addr...)
	}
}

func SetBalancer(balancer kafka.Balancer) Setter {
	return func(p *Producer) {
		p.balancer = balancer
	}
}

func SetTopicList(topic ...string) Setter {
	return func(p *Producer) {
		p.topicList = topic
	}
}

func SetMaxAttempts(maxAttempts int) Setter {
	return func(p *Producer) {
		p.maxAttempts = maxAttempts
	}
}

func SetWriteBackoffMin(writeBackoffMin time.Duration) Setter {
	return func(p *Producer) {
		p.writeBackoffMin = writeBackoffMin
	}
}

func SetWriteBackoffMax(writeBackoffMax time.Duration) Setter {
	return func(p *Producer) {
		p.writeBackoffMax = writeBackoffMax
	}
}

func SetBatchSize(batchSize int) Setter {
	return func(p *Producer) {
		p.batchSize = batchSize
	}
}

func SetBatchBytes(batchBytes int64) Setter {
	return func(p *Producer) {
		p.batchBytes = batchBytes
	}
}

func SetBatchTimeout(batchTimeout time.Duration) Setter {
	return func(p *Producer) {
		p.batchTimeout = batchTimeout
	}
}

func SetReadTimeout(readTimeout time.Duration) Setter {
	return func(p *Producer) {
		p.readTimeout = readTimeout
	}
}

func SetWriteTimeout(writeTimeout time.Duration) Setter {
	return func(p *Producer) {
		p.writeTimeout = writeTimeout
	}
}

func SetRequiredAcks(requiredAcks kafka.RequiredAcks) Setter {
	return func(p *Producer) {
		p.requiredAcks = requiredAcks
	}
}

func SetCompletion(completion func(messages []kafka.Message, err error)) Setter {
	return func(p *Producer) {
		p.completion = completion
	}
}

func SetCompression(compression kafka.Compression) Setter {
	return func(p *Producer) {
		p.compression = compression
	}
}

func SetLogger(logger logger.LogWriter) Setter {
	return func(p *Producer) {
		p.logger = logger
	}
}

func SetTransport(transport kafka.RoundTripper) Setter {
	return func(p *Producer) {
		p.transport = transport
	}
}

func SetAllowAutoTopicCreation(allowAutoTopicCreation bool) Setter {
	return func(p *Producer) {
		p.allowAutoTopicCreation = allowAutoTopicCreation
	}
}

func SetMsgType(msgType message.MsgType) Setter {
	return func(p *Producer) {
		p.msgType = msgType
	}
}
