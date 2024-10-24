package rabbitmq

import (
	"context"
	"github.com/2015wuji01/deck"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type ConsumerConfig struct {
	Qos struct {
		prefetchCount int
		prefetchSize  int
		global        bool
	}

	AckHandler func(ctx context.Context, msg amqp.Acknowledger) error
}

func NewDefaultConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		Qos: struct {
			prefetchCount int
			prefetchSize  int
			global        bool
		}{
			prefetchCount: 20,
			prefetchSize:  0,
			global:        false,
		},
		AckHandler: nil,
	}
}

type ConfigOption func(config ConsumerConfig) error

func SetQos(prefetchCount int, prefetchSize int, global bool) ConfigOption {
	return func(config ConsumerConfig) error {
		config.Qos.prefetchCount = prefetchCount
		config.Qos.prefetchSize = prefetchSize
		config.Qos.global = global
		return nil
	}
}

func SetPrefetchCount(prefetchCount int) ConfigOption {
	return func(config ConsumerConfig) error {
		config.Qos.prefetchCount = prefetchCount
		return nil
	}
}

func SetAckHandler(ackHandler func(ctx context.Context, acknowledger amqp.Acknowledger) error) ConfigOption {
	return func(config ConsumerConfig) error {
		config.AckHandler = ackHandler
		return nil
	}
}

type Consumer struct {
	cli *Client

	ctx    context.Context
	cancel context.CancelFunc

	config ConsumerConfig
	ch     *amqp.Channel

	run func(ctx context.Context, msg amqp.Delivery) error
}

func (cli *Client) NewConsumer(opts ...ConfigOption) (deck.Consumer, error) {
	config := NewDefaultConsumerConfig()
	for _, opt := range opts {
		if err := opt(config); err != nil {
			return nil, err
		}
	}

	ctx, cancel := context.WithCancel(cli.ctx)
	consumer := &Consumer{
		cli:    cli,
		ctx:    ctx,
		cancel: cancel,
		config: config,
	}

	err := consumer.ResetChannel()
	if err != nil {
		return nil, err
	}
	err = consumer.ch.Qos(config.Qos.prefetchCount, config.Qos.prefetchSize, config.Qos.global)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

func (c *Consumer) ResetChannel() error {
	if c.ch != nil {
		_ = c.ch.Close()
	}
	ch, err := c.cli.conn.Channel()
	if err != nil {
		return err
	}
	c.ch = ch
	//go func() {
	//	shutdown := c.ch.NotifyClose(make(chan *amqp.Error))
	//	e := <-shutdown
	//	c.cli.l.Info(c.cli.ctx, "channel 关闭 %+v", e)
	//	c.Stop()
	//}()
	return nil
}

func (c *Consumer) Consume(ctx context.Context, queue string, handler func(source <-chan any)) error {
	if queue == "" {
		c.cli.l.Error(c.cli.ctx, "队列名称不能为空")
		return EmptyQueueNameErr
	}

	// channel 关闭，自动重新获取 channel
	if c.ch == nil || c.ch.IsClosed() {
		if err := c.ResetChannel(); err != nil {
			return err
		}
	}

	// 1.申请队列，如果队列不存在会自动创建，存在则跳过创建
	q, err := c.QueueDeclare(queue, nil)
	if err != nil {
		return err
	}

	// 2.启动消费者
	source := make(chan any)
	go func() {
		handler(source)
	}()

	// 接收消息
	for {
		// channel 关闭，自动重新获取 channel
		if c.ch == nil || c.ch.IsClosed() {
			//c.cli.l.Info(c.cli.ctx, "检查到 channel 关闭，尝试重新获取 channel")
			if err := c.ResetChannel(); err != nil {
				c.cli.l.Warn(c.cli.ctx, "获取 channel 失败，等待 1s 后重试: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}
			c.cli.l.Info(c.cli.ctx, "获取 channel 成功")
		}
		msgs, err := c.ch.Consume(q.Name, "", false, false, false, false, nil)
		if err != nil {
			//c.cli.l.Warn(c.cli.ctx, "消费者启动失败，等待 1s 后重试: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

	LOOP:
		for {
			select {
			case <-c.ctx.Done():
				c.cli.l.Info(c.cli.ctx, "consumer exit successfully")
				return nil
			case <-ctx.Done():
				c.cli.l.Info(c.cli.ctx, "接收到 context.Done 信号，退出接收消息")
				return nil
			case msg, ok := <-msgs:
				if !ok {
					break LOOP
				}
				source <- msg
			}
		}
	}
}

// QueueDeclare 声明队列
// name 队列名称
// durable 是否持久化
// autoDelete 是否自动删除
// exclusive 是否具有排他性
// noWait 是否阻塞处理
// args 额外的属性
func (c *Consumer) QueueDeclare(name string, args map[string]interface{}) (amqp.Queue, error) {
	q, err := c.ch.QueueDeclare(name, true, false, false, false, args)
	if err != nil {
		return q, err
	}
	return q, nil
}

func (c *Consumer) Close() error {
	return c.ch.Close()
}

func (c *Consumer) Stop() {
	c.cancel()
}
