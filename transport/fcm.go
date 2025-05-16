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

func (f *FCMTransport) SendMessage(ctx context.Context, message *RoomMessage) error {
	fcmMessage := &messaging.Message{
		Data:  message.Data,
		Topic: message.Topic(),
		APNS: &messaging.APNSConfig{
			Headers: map[string]string{
				"apns-priority": "5",
			},
			Payload: &messaging.APNSPayload{
				Aps: &messaging.Aps{
					ContentAvailable: true,
				},
			},
		},
	}
	return f.SendFCMMessage(ctx, fcmMessage)
}
func (f *FCMTransport) SendFCMMessage(ctx context.Context, message *messaging.Message) error {
	response, err := f.fcm.Send(ctx, message)
	if err != nil {
		log.Printf("error sending FCM message: %v\n", err)
	} else {
		log.Printf("Successfully sent message: %s\n", response)
	}
	return err
}
