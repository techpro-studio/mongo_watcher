package mongo_watcher

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/techpro-studio/gocache"
	"github.com/techpro-studio/gomongo"
	"github.com/techpro-studio/mongo_watcher/transport"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
)

type MongoSchema[T any] interface {
	GetId() primitive.ObjectID
	gomongo.ModelConverted[T]
}

type CollectionWatcher[T any, M MongoSchema[T]] struct {
	collection                 *mongo.Collection
	cache                      gocache.TypedCache[T]
	trans                      transport.Transport
	preheatCache               bool
	dispatchUpdateInGlobalRoom bool
	roomPrefix                 string
}

func NewCollectionWatcher[T any, M MongoSchema[T]](collection *mongo.Collection, cache gocache.TypedCache[T], trans transport.Transport, roomPrefix string, dispatchUpdateInGlobalRoom bool, preheatCache bool) *CollectionWatcher[T, M] {
	return &CollectionWatcher[T, M]{collection: collection, cache: cache, trans: trans, roomPrefix: roomPrefix, dispatchUpdateInGlobalRoom: dispatchUpdateInGlobalRoom, preheatCache: preheatCache}
}

func (c *CollectionWatcher[T, M]) Watch(ctx context.Context) {

	if c.preheatCache {
		find, err := c.collection.Find(ctx, bson.M{})
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

	changeStreamOptions := options.ChangeStream().SetFullDocument(options.UpdateLookup)

	pipeline := mongo.Pipeline{
		{{"$match", bson.D{
			{"operationType", bson.D{{"$in", bson.A{"insert", "update", "replace", "delete"}}}},
		}}},
	}
	changeStream, err := c.collection.Watch(ctx, pipeline, changeStreamOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer func(changeStream *mongo.ChangeStream, ctx context.Context) {
		err := changeStream.Close(ctx)
		if err != nil {
			log.Printf("Error closing change stream: %v\n", err)
		}
	}(changeStream, ctx)

	log.Printf("Start watching %s", c.collection.Name())

	for changeStream.Next(ctx) {
		var event struct {
			FullDocument M      `bson:"fullDocument"`
			Type         string `bson:"operationType"`
			DocumentKey  struct {
				ID primitive.ObjectID `bson:"_id"`
			} `bson:"documentKey"`
		}
		if err := changeStream.Decode(&event); err != nil {
			log.Printf("error decoding change stream: %v\n", err)
			continue
		}

		key := event.DocumentKey.ID.Hex()

		var sendData map[string]string

		switch event.Type {
		case "insert", "update", "replace":
			// Handle insert, update, and replace by caching the document
			data := *event.FullDocument.ToModel()

			log.Printf("Got %s %s:%s", event.Type, c.roomPrefix, key)

			err := c.cache.Set(ctx, key, data)
			if err != nil {
				log.Printf("Failed to cache %s: %v\n", event.Type, err)
				continue
			}

			jsonData, err := json.Marshal(data)
			if err != nil {
				log.Printf("Error marshalling data to JSON: %v", err)
				continue
			}

			sendData = map[string]string{"payload": string(jsonData)}

		case "delete":
			// Handle delete by removing the key from the cache

			log.Printf("Got delete event %s:%s", c.roomPrefix, key)

			err := c.cache.Delete(ctx, key)
			if err != nil {
				log.Printf("Failed to remove from cache: %v\n", err)
			}

			sendData = map[string]string{"id": key}

		default:
			log.Printf("Unhandled operationType: %s", event.Type)
		}

		err = c.trans.SendMessage(ctx, &transport.Message{
			Data:  sendData,
			Room:  fmt.Sprintf("%s.%s", c.roomPrefix, key),
			Event: event.Type,
		})
		if err != nil {
			log.Printf("Failed to send %s event: %v\n", event.Type, err)
			continue
		}

		if c.dispatchUpdateInGlobalRoom {
			err = c.trans.SendMessage(ctx, &transport.Message{
				Data:  sendData,
				Room:  c.roomPrefix,
				Event: event.Type,
			})
			if err != nil {
				log.Printf("Failed to send global update: %v\n", err)
				continue
			}
		}
	}
}
