package rabbitmq

import (
	"encoding/json"
	"github.com/2015wuji01/deck"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ProducerConfig struct {
	Exchange string
	//Queue    string

	ReuseChannel  bool
	MaxReuseCount uint
	Concurrency   uint
}

type Producer struct {
	cli      *Client
	exchange string
	//queue    string

	// 并发数，每个并发独占一个 channel，即允许同时有多少个 channel 存在
	//concur uint

	// 假设提供一个 channel 连接池
	reuse    bool
	maxReuse uint
	ch       *amqp.Channel
	workers  int
}

// NewProducer 创建一个 Producer
//
//	rabbitmq driver 需要解决的问题：
//
//	Q1. 如何知道消息发到哪里去？
//	A1. 可以通过 exchange 和 queue 来确定，必填性与原生 rabbitmq 一致，不填则随机生成一个 queue
//
//	Q2. 如何并发发送消息，并且需要保证连接数不会过多？如果每次发送消息都创建一个连接，那么连接数会过多，所以需要一个连接池，这个连接池怎么实现？channel 在 rabbitmq 中是一个轻量级的连接，即不应该常驻，需要用完就关闭。不知道 sync.channelPool 能不能用在这里？
//	A2. 尝试加了一个 sync.channelPool，发送速度明显加快。定义了一个 重用量，即一个 channel 最多重用多少次，经过测试，在本机发现 50 达到峰值，所以暂定 50，对于不同机器可能需要调整，这个值最好需要开放出来，让用户自己调整。
func (cli *Client) NewProducer(c ProducerConfig) (deck.Producer, error) {
	if c.Concurrency <= 0 {
		c.Concurrency = 1
	}

	if c.MaxReuseCount == 0 {
		c.MaxReuseCount = 50
	}

	return &Producer{
		cli:      cli,
		exchange: c.Exchange,
		//queue:    c.Queue,
		reuse:    c.ReuseChannel,
		maxReuse: c.MaxReuseCount,
		workers:  int(c.Concurrency),
	}, nil
}

func (p *Producer) Send(queue string, source <-chan any, errHandler func(item any, err error)) error {
	if queue == "" {
		return EmptyQueueNameErr
	}

	err := p.ResetChannel()
	if err != nil {
		return err
	}

	q, err := p.QueueDeclare(queue, nil)
	if err != nil {
		return err
	}

	for item := range source {
		pub, ok := item.(amqp.Publishing)
		if !ok {
			if errHandler != nil {
				errHandler(item, ErrNotAMQPMessage)
			}
			continue
		}

		err = p.ch.Publish(p.exchange, q.Name, false, false, pub)
		if err != nil {
			if errHandler != nil {
				errHandler(item, err)
			}
		}
	}
	return nil
}

func (p *Producer) Close() error {
	return p.ch.Close()
}

func (p *Producer) ResetChannel() error {
	if p.ch != nil {
		_ = p.ch.Close()
	}
	ch, err := p.cli.conn.Channel()
	if err != nil {
		return err
	}
	p.ch = ch
	return nil
}

// QueueDeclare 声明队列
// name 队列名称
// durable 是否持久化
// autoDelete 是否自动删除
// exclusive 是否具有排他性
// noWait 是否阻塞处理
// args 额外的属性
func (p *Producer) QueueDeclare(name string, args map[string]interface{}) (amqp.Queue, error) {
	q, err := p.ch.QueueDeclare(name, true, false, false, false, args)
	if err != nil {
		return q, err
	}
	return q, nil
}

func NewJsonMessage(item any) amqp.Publishing {
	b, _ := json.Marshal(item)
	return amqp.Publishing{Body: b}
}

func NewStringMessage(s string) amqp.Publishing {
	return amqp.Publishing{Body: []byte(s)}
}

func NewBytesMessage(b []byte) amqp.Publishing {
	return amqp.Publishing{Body: b}
}
