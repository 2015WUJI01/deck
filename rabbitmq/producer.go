package rabbitmq

import (
	"encoding/json"
	"fmt"
	"github.com/2015wuji01/go-deck"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/zeromicro/go-zero/core/fx"
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

func (p *Producer) Send(queue string, stream fx.Stream, errHandler func(item any, err error)) error {
	err := p.ResetChannel()
	if err != nil {
		return err
	}

	q, err := p.QueueDeclare(queue, nil)
	if err != nil {
		return err
	}

	stream.Map(func(item any) any {
		switch item.(type) {
		case []byte:
			return item.([]byte)
		case string:
			return []byte(item.(string))
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool:
			return []byte(fmt.Sprintf("%v", item))
		default:
			b, err := json.Marshal(item)
			if err != nil {
				if errHandler != nil {
					errHandler(item, err)
				}
				return nil
			}
			return b
		}
	}, fx.WithWorkers(p.workers)).ForEach(func(item any) {
		body, ok := item.([]byte)
		if !ok {
			if errHandler != nil {
				errHandler(item, ErrNotBytes)
			}
			return
		}

		err = p.ch.Publish(p.exchange, q.Name, false, false, amqp.Publishing{
			Body: body,
		})
		if err != nil {
			if errHandler != nil {
				errHandler(item, err)
			}
		}
	})
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

// // NewProducer 创建一个 Producer
// //
// //	rabbitmq driver 需要解决的问题：
// //
// //	Q1. 如何知道消息发到哪里去？
// //	A1. 可以通过 exchange 和 queue 来确定，必填性与原生 rabbitmq 一致，不填则随机生成一个 queue
// //
// //	Q2. 如何并发发送消息，并且需要保证连接数不会过多？如果每次发送消息都创建一个连接，那么连接数会过多，所以需要一个连接池，这个连接池怎么实现？channel 在 rabbitmq 中是一个轻量级的连接，即不应该常驻，需要用完就关闭。不知道 sync.channelPool 能不能用在这里？
// //	A2. 尝试加了一个 sync.channelPool，发送速度明显加快。定义了一个 重用量，即一个 channel 最多重用多少次，经过测试，在本机发现 50 达到峰值，所以暂定 50，对于不同机器可能需要调整，这个值最好需要开放出来，让用户自己调整。
func (cli *Client) NewProducer(c ProducerConfig) (deck.Producer, error) {
	if c.Concurrency <= 0 {
		c.Concurrency = 1
	}

	//if c.Queue == "" {
	//	return nil, EmptyQueueNameErr
	//}

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

//func (p *Producer) sendMessage(ctx context.Context, msg *message.Message) error {
//	ch, err := p.getChannel()
//	if err != nil {
//		return err
//	}
//
//	q, err := ch.QueueDeclare(p.queue, true, false, false, false, nil)
//	if err != nil {
//		return err
//	}
//	//defer p.releaseChannel(ch)
//	m := amqp.Publishing{Body: msg.Value, Headers: amqp.Table{
//		"x-message-ttl": 3 * time.Second.Milliseconds(),
//	}}
//	m.Body = []byte(fmt.Sprintf("%d", ch.reused))
//	return ch.PublishWithContext(ctx, p.exchange, q.Name, false, false, m)
//}
//
//// SendBytes 发送 Bytes 格式的消息
//func (p *Producer) SendBytes(ctx context.Context, key string, value []byte) error {
//	return p.sendMessage(ctx, &message.Message{Key: key, Value: value})
//}
//
//// SendString 发送 string 格式的消息
//func (p *Producer) SendString(ctx context.Context, key string, value string) error {
//	return p.sendMessage(ctx, &message.Message{Key: key, Value: []byte(value)})
//}
//
//// SendJSON json 格式编码，非 json 格式请转为 json，或者用 SendBytes 与 SendString 代替
//func (p *Producer) SendJSON(ctx context.Context, key string, value any) error {
//	b, err := json.Marshal(value) // todo: json 可以换成更快的包
//	if err != nil {
//		return err
//	}
//	return p.sendMessage(ctx, &message.Message{Key: key, Value: b})
//}
//
//// SendBatch 批量发送消息
//func (p *Producer) SendBatch(ctx context.Context, msgs []*message.Message) error {
//	g, ctx := errgroup.WithContext(ctx)
//	g.SetLimit(int(p.concur))
//	for _, m := range msgs {
//		msg := m
//		g.Go(func() error {
//			return p.sendMessage(ctx, msg)
//		})
//	}
//	return g.Wait()
//}
//
//// GetChannel 从对象池中获取一个 Channel
//func (p *Producer) getChannel() (*channel, error) {
//	return p.pool, nil
//	//if !p.reuse {
//	//	return newChannel(p.cli)
//	//}
//	//v := p.pool.Get()
//	//if _, ok := v.(error); ok {
//	//	return newChannel(p.cli)
//	//}
//	//return v.(*channel), nil
//}
//
//// ReleaseChannel 将 Channel 放回对象池
//func (p *Producer) releaseChannel(c *channel) error {
//	if p.reuse && atomic.AddInt64(&c.reused, 1) < int64(p.maxReuse) {
//		// 将 Channel 放回对象池
//		p.pool.Put(c)
//		log.Printf("release channel: %v", c.reused)
//		return nil
//	}
//	log.Printf("close channel: %v < %v", c.reused, p.maxReuse)
//	return c.Close()
//}
