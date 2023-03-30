package message

import (
	"encoding/json"
	"github.com/google/uuid"
	"testing"
)

func TestMessage(t *testing.T) {
	var pm Message
	pm.Msg = "test"
	pm.TrackID = uuid.New().String()
	b, err := json.Marshal(pm)
	if err != nil {
		t.Fatal(err)
	}
	var cm ConsumerMessage
	err = json.Unmarshal(b, &cm)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(cm.Msg))
}
