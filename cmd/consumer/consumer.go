package main

import (
  "encoding/json"
  "errors"
  "fmt"
  "log"
  "net/http"
  "sync"
  "context"

  "kafka-notify/pkg/models"

  "github.com/IBM/sarama"
  "github.com/gin-gonic/gin"
)

const (
  ConsumerGroup = "notifications-group"
  ConsumerTopic = "notifications"
  ConsumerPort = ":8081"
  KafkaServerAdress = "localhost:9092"
)

var ErrNoMessageFound = errors.New("no messages found")

func getUserIDFromRequest(ctx *gin.Context) (string, error) {
  userID := ctx.Param("userID")
  if userID == "" {
    return "", ErrNoMessageFound
  }

  return userID, nil
}

type UserNotifications map[string][]models.Notification
type NotificationStore struct {
  data UserNotifications
  mu sync.RWMutex
}

func (ns *NotificationStore) Add(userID string, notification models.Notification) {
  ns.mu.Lock()
  defer ns.mu.Unlock()

  ns.data[userID] = append(ns.data[userID], notification)
}

func (ns *NotificationStore) Get(userID string) []models.Notification {
  ns.mu.RLock()
  defer ns.mu.RUnlock()

  return ns.data[userID]
}

type Consumer struct {
  store *NotificationStore
}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error { return nil }
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
  for message := range claim.Messages() {
    userID := string(message.Key)
    var notification models.Notification
    err := json.Unmarshal(message.Value, &notification)
    if err != nil {
      log.Printf("failed to unmarshal message: %v", err)
      continue
    }

    consumer.store.Add(userID, notification)
    session.MarkMessage(message, "")
  }
  return nil
}

func initializeConsumerGroup() (sarama.ConsumerGroup, error) {
  config := sarama.NewConfig()

  consumerGroup, err := sarama.NewConsumerGroup([]string{KafkaServerAdress}, ConsumerGroup, config)
  if err != nil {
    return nil, fmt.Errorf("failed to create consumer group: %w", err)
  }
  return consumerGroup, nil
}

func setupConsumerGroup(ctx context.Context, store *NotificationStore) {
  consumerGroup, err := initializeConsumerGroup()
  if err != nil {
    log.Fatalf("failed to initialize consumer group: %v", err)
  }
  defer consumerGroup.Close()

  consumer := &Consumer{store: store}

  for {
    err := consumerGroup.Consume(ctx, []string{ConsumerTopic}, consumer)
    if err != nil {
      log.Printf("failed to consume messages: %v", err)
    }
    if ctx.Err() != nil {
      return
    }
  }
}

func handleNotifications(ctx *gin.Context, store *NotificationStore) {
  userID, err := getUserIDFromRequest(ctx)
  if userID == "" {
    ctx.JSON(http.StatusNotFound, gin.H{"message": err.Error()})
    return
  }

  notes := store.Get(userID)
  if len(notes) == 0 {
    ctx.JSON(http.StatusOK, gin.H{
      "message": "No notifications found",
      "notifications": []models.Notification{},
    })
    return
  }
  ctx.JSON(http.StatusOK, gin.H{"notifications": notes})
}

func main() {
  store := &NotificationStore{data: make(UserNotifications)}
  ctx, cancel := context.WithCancel(context.Background())
  go setupConsumerGroup(ctx, store)
  defer cancel()

  router := gin.Default()
  router.GET("/notifications/:userID", func(ctx *gin.Context) {
    handleNotifications(ctx, store)
  })

  fmt.Printf("Kafka CONSUMER (Group: %s) 👥📥 started at http://localhost%s\n", ConsumerGroup, ConsumerPort)
  if err := router.Run(ConsumerPort); err != nil {
    log.Fatalf("failed to run router: %v", err)
  }
}
