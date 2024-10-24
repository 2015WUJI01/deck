package deck

type Producer interface {
	// Send 将消息发送到消息队列中
	Send(queue string, source <-chan any, errHandler func(item any, err error)) error

	// Close 用于关闭生产者连接，释放资源
	Close() error
}
