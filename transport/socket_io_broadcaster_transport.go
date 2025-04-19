package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

type SocketIoBroadcasterTransport struct {
	BaseUrl string
	Key     string
}

func NewSocketIoBroadcasterTransport(baseUrl, key string) *SocketIoBroadcasterTransport {
	return &SocketIoBroadcasterTransport{
		BaseUrl: baseUrl,
		Key:     key,
	}
}

func (t *SocketIoBroadcasterTransport) SendMessage(ctx context.Context, message *Message) error {
	message.Data["topic"] = message.Topic()
	final := map[string]any{
		"event": message.Event,
		"data":  message.Data,
		"room":  message.Room,
	}

	log.Printf("Sending message %v", message)

	body, err := json.Marshal(final)

	if err != nil {
		return err
	}

	bodyReader := bytes.NewReader(body)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/dispatch", t.BaseUrl), bodyReader)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("key", t.Key)
	client := http.Client{Timeout: 10 * time.Second}

	res, err := client.Do(req)
	if err != nil {
		return err
	}
	resBody, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}
	var respData map[string]any
	err = json.Unmarshal(resBody, &respData)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return errors.New(respData["error"].(string))
	}

	return nil
}
