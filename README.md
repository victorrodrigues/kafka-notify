# Notification System using Kafka + Go

### Sending messages

User 1 (Emma) receiving message from User 2 (Bruno):

```bash
curl -X POST http://localhost:8080/send -d "fromID=2&toID=1&message=Bruno started following you."
```

User 2 (Bruno) receives a notification from User 1 (Emma)

```bash
curl -X POST http://localhost:8080/send \
-d "fromID=1&toID=2&message=Emma mentioned you in a comment: 'Great seeing you yesterday, @Bruno!'"
```

User 1 (Emma) receives a notification from User 4 (Lena)
```bash
curl -X POST http://localhost:8080/send \
-d "fromID=4&toID=1&message=Lena liked your post: 'My weekend getaway!'"
```

### Retrieving notifications

*Retrieving notifications for User 1 (Emma):*
```bash
curl http://localhost:8081/notifications/1
```

