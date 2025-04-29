package transport

import (
	"context"
	"encoding/base64"
	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"google.golang.org/api/option"
	"log"
)

type FCMTransport struct {
	app *firebase.App
	fcm *messaging.Client
}

func NewFCMTransport(ctx context.Context, firebasePrivateKeyB64 string) (*FCMTransport, error) {
	var err error

	credJSON, err := base64.StdEncoding.DecodeString(firebasePrivateKeyB64)
	if err != nil {
		return nil, err
	}

	opt := option.WithCredentialsJSON(credJSON)

	app, err := firebase.NewApp(ctx, nil, opt)

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
