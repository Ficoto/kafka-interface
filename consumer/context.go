package consumer

import (
	"context"
	"encoding/json"
	"github.com/Ficoto/kafka-interface/message"
	"github.com/google/uuid"
	"time"
)

type Context struct {
	ctx     context.Context
	message *message.ConsumerMessage
}

func (ctx *Context) Deadline() (deadline time.Time, ok bool) {
	return ctx.ctx.Deadline()
}

func (ctx *Context) Done() <-chan struct{} {
	return ctx.ctx.Done()
}

func (ctx *Context) Err() error {
	return ctx.ctx.Err()
}

func (ctx *Context) Value(key any) any {
	if keyStr, ok := key.(string); ok && keyStr == "track_id" {
		return ctx.message.TrackID
	}
	return ctx.Value(key)
}

func (ctx *Context) Reset() {
	ctx.ctx = context.Background()
	ctx.message = nil
}

func (ctx *Context) BindJson(obj any) error {
	return json.Unmarshal(ctx.message.Msg, obj)
}

func GenerateCTXByByte(ctx context.Context, b []byte, msgType message.MsgType) (Context, error) {
	var c Context
	c.ctx = ctx
	switch msgType {
	case message.DefaultMsgType:
		c.message = new(message.ConsumerMessage)
		c.message.Msg = b
		c.message.TrackID = uuid.New().String()
	case message.WitchTrackMsgType:
		c.message = new(message.ConsumerMessage)
		err := json.Unmarshal(b, c.message)
		if err != nil {
			return c, err
		}
	}
	return c, nil
}
