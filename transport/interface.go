package transport

import (
	"context"
)

type Message struct {
	Room  string            `json:"room"`
	Event string            `json:"event"`
	Data  map[string]string `json:"data"`
}

func (m *Message) Topic() string {
	return m.Room
}

type Transport interface {
	SendMessage(ctx context.Context, message *Message) error
}
