package mongo_watcher

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/techpro-studio/mongo_watcher/transport"
	"go.mongodb.org/mongo-driver/mongo"
	"log"
)

type TransportEventHandler[T any] struct {
	trans      transport.RoomTransport
	roomFormat string
}

func NewTransportEventHandler[T any](trans transport.RoomTransport, roomFormat string) *TransportEventHandler[T] {
	return &TransportEventHandler[T]{trans: trans, roomFormat: roomFormat}
}

func (t *TransportEventHandler[T]) Setup(ctx context.Context, collection *mongo.Collection) {
	log.Printf("Setting up transport event handler for %s", collection.Name())
}

func (t *TransportEventHandler[T]) HandleEvent(ctx context.Context, event *Event[T]) error {
	var sendData map[string]string
	switch event.Type {
	case MongoEventDelete:
		sendData = map[string]string{"id": event.Key}
	default:
		jsonData, err := json.Marshal(*event.FullDocument)
		if err != nil {
			return err
		}
		sendData = map[string]string{"payload": string(jsonData)}
	}

	err := t.trans.SendMessage(ctx, &transport.RoomMessage{
		Data:  sendData,
		Room:  fmt.Sprintf(t.roomFormat, event.Key),
		Event: string(event.Type),
	})
	if err != nil {
		return err
	}

	return nil
}
