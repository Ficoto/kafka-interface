package saramaproducer

import (
	"github.com/Ficoto/kafka-interface/message"
	"github.com/Shopify/sarama"
)

type Setter func(p *Producer)

func SetSaramaConfig(config *sarama.Config) Setter {
	return func(p *Producer) {
		p.config = config
	}
}

func SetAddr(addr ...string) Setter {
	return func(p *Producer) {
		p.addrs = addr
	}
}

func SetMsgType(msgType message.MsgType) Setter {
	return func(p *Producer) {
		p.msgType = msgType
	}
}
