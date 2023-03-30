package message

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
)

type MsgType uint16

const (
	DefaultMsgType MsgType = iota
	WitchTrackMsgType
)

type Message any

type ConsumerMessage struct {
	Msg     json.RawMessage `json:"msg"`
	TrackID string          `json:"track_id"`
}

type MsgWithTrack struct {
	Msg     any    `json:"msg"`
	TrackID string `json:"track_id"`
}

func MsgToByte(ctx context.Context, msg any, msgType MsgType) ([]byte, error) {
	var (
		b   []byte
		err error
	)
	switch msgType {
	case DefaultMsgType:
		b, err = json.Marshal(msg)
	case WitchTrackMsgType:
		trackID := ctx.Value("track_id")
		trackIDStr, ok := trackID.(string)
		if !ok || len(trackIDStr) == 0 {
			trackIDStr = uuid.New().String()
		}
		b, err = json.Marshal(MsgWithTrack{
			Msg:     msg,
			TrackID: trackIDStr,
		})
	}
	return b, err
}
