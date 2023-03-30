package producer

import (
	"context"
)

type Producer interface {
	AsyncSendMsg(ctx context.Context, topic string, key string, msg any) error
	SyncSendMsg(ctx context.Context, topic string, key string, msg any) error
}
