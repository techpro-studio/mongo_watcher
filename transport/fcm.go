package transport

import (
	"context"
	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"log"
)

type FCMTransport struct {
	app *firebase.App
	fcm *messaging.Client
}

func NewFCMTransport(ctx context.Context) (*FCMTransport, error) {
	var err error

	app, err := firebase.NewApp(ctx, nil, nil)

	if err != nil {
		return nil, err
	}

	fcm, err := app.Messaging(ctx)

	if err != nil {
		return nil, err
	}
	return &FCMTransport{app: app, fcm: fcm}, nil
}

func (f *FCMTransport) SendMessage(ctx context.Context, message *Message) error {
	fcmMessage := &messaging.Message{
		Data:  message.Data,
		Topic: message.Topic(),
	}
	response, err := f.fcm.Send(ctx, fcmMessage)
	if err != nil {
		log.Printf("error sending FCM message: %v\n", err)
	} else {
		log.Printf("Successfully sent message: %s\n", response)
	}
	return err
}
