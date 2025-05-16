package transport

import (
	"context"
	"github.com/tideland/golib/errors"
)

type CombinedTransport struct {
	children []RoomTransport
}

func NewCombinedTransport() *CombinedTransport {
	return &CombinedTransport{children: []RoomTransport{}}
}

func (c *CombinedTransport) AddChild(t RoomTransport) {
	c.children = append(c.children, t)
}

func (c *CombinedTransport) Count() int {
	return len(c.children)
}

func (c *CombinedTransport) SendMessage(ctx context.Context, message *RoomMessage) error {
	var errs []error
	for _, child := range c.children {
		if err := child.SendMessage(ctx, message); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Collect(errs...)
	}
	return nil
}
