package transport

import (
	"context"
)

type RoomMessage struct {
	Room  string            `json:"room"`
	Event string            `json:"event"`
	Data  map[string]string `json:"data"`
}

func (m *RoomMessage) Topic() string {
	return m.Room
}

type RoomTransport interface {
	SendMessage(ctx context.Context, message *RoomMessage) error
}
