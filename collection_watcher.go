package mongo_watcher

import (
	"context"
	"github.com/techpro-studio/gomongo"
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

type MongoEvent string

const MongoEventInsert MongoEvent = "insert"
const MongoEventUpdate MongoEvent = "update"
const MongoEventDelete MongoEvent = "delete"
const MongoEventReplace MongoEvent = "replace"

type Event[T any] struct {
	Type         MongoEvent
	FullDocument *T
	Key          string
}

type EventHandler[T any] interface {
	Setup(ctx context.Context, collection *mongo.Collection)
	HandleEvent(ctx context.Context, event *Event[T]) error
}

type CollectionWatcher[T any, M MongoSchema[T]] struct {
	collection    *mongo.Collection
	eventHandlers []EventHandler[T]
}

func NewCollectionWatcher[T any, M MongoSchema[T]](collection *mongo.Collection, eventHandlers []EventHandler[T]) *CollectionWatcher[T, M] {
	return &CollectionWatcher[T, M]{collection: collection, eventHandlers: eventHandlers}
}

func (c *CollectionWatcher[T, M]) Watch(ctx context.Context) {

	changeStreamOptions := options.ChangeStream().SetFullDocument(options.UpdateLookup)

	pipeline := mongo.Pipeline{
		{{"$match", bson.D{
			{"operationType", bson.D{{"$in", bson.A{MongoEventInsert, MongoEventUpdate, MongoEventReplace, MongoEventDelete}}}},
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

		var toHandle Event[T]

		switch MongoEvent(event.Type) {
		case MongoEventInsert, MongoEventUpdate, MongoEventReplace:
			// Handle insert, update, and replace by caching the document
			data := *event.FullDocument.ToModel()

			toHandle = Event[T]{
				Type:         MongoEvent(event.Type),
				FullDocument: &data,
				Key:          key,
			}

		case MongoEventDelete:
			toHandle = Event[T]{
				Type: MongoEvent(event.Type),
				Key:  key,
			}
		default:
			log.Printf("Unhandled operationType: %s", event.Type)
		}

		for _, handler := range c.eventHandlers {
			err := handler.HandleEvent(ctx, &toHandle)
			if err != nil {
				log.Printf("Error handling event: %v\n", err)
			}
		}

	}
}
