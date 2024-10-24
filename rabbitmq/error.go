package rabbitmq

import "fmt"

var EmptyQueueNameErr = fmt.Errorf("queue name is empty")
var ErrNotBytes = fmt.Errorf("message is not of type []byte")
var ErrNotAMQPMessage = fmt.Errorf("item is not an amqp.Publishing message")
