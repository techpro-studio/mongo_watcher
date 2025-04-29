package mongo_watcher

import (
	"context"
	"github.com/techpro-studio/gocache"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"log"
)

type CacheEventHandler[T any, M MongoSchema[T]] struct {
	cache   gocache.TypedCache[T]
	preheat bool
}

func NewCacheEventHandler[T any, M MongoSchema[T]](cache gocache.TypedCache[T], preheat bool) *CacheEventHandler[T, M] {
	return &CacheEventHandler[T, M]{cache: cache, preheat: preheat}
}

func (c *CacheEventHandler[T, M]) Setup(ctx context.Context, collection *mongo.Collection) {
	if !c.preheat {
		return
	}

	find, err := collection.Find(ctx, bson.M{})
	if err != nil {
		return
	}
	var all []M
	err = find.All(ctx, &all)
	if err != nil {
		log.Fatalf("failed to preheat cache. fetch all: %v", err)
	}
	for _, m := range all {
		err := c.cache.Set(ctx, m.GetId().Hex(), *m.ToModel())
		if err != nil {
			log.Fatalf("failed to cache item: %v", err)
		}
	}
}

func (c *CacheEventHandler[T, M]) HandleEvent(ctx context.Context, event *Event[T]) error {
	switch event.Type {
	case MongoEventDelete:
		return c.cache.Delete(ctx, event.Key)
	default:
		return c.cache.Set(ctx, event.Key, *event.FullDocument)
	}
}
