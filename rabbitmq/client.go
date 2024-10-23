package rabbitmq

import (
	"context"
	"github.com/cenkalti/backoff/v4"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

type ClientConfig struct {
	// 日志输出
	Logger *log.Logger

	// 重连检测的时间间隔，默认 1s
	Heartbeat time.Duration
	// 重连失败时的回调函数，可以在这里打印日志
	RetryNotify backoff.Notify
}

type Client struct {
	source string
	ctx    context.Context
	cfg    ClientConfig
	conn   *amqp.Connection

	l *log.Logger
}

func NewClient(source string, cfg ClientConfig) (*Client, error) {
	var cli = &Client{
		source: source,
		ctx:    context.Background(),
		cfg:    cfg,
		l:      cfg.Logger,
	}
	if cli.l == nil {
		cli.l = log.New(log.Writer(), "[rabbitmq driver]\t", log.LstdFlags)
	}

	var err error
	cli.conn, err = newConnection(source, cfg)
	if err != nil {
		cli.l.Printf("failed to connect to rabbitmq: %v", err)
		return nil, err
	}
	go cli.keepAlive()
	return cli, nil
}

func newConnection(source string, cfg ClientConfig) (conn *amqp.Connection, err error) {
	return amqp.DialConfig(source, amqp.Config{
		Heartbeat: cfg.Heartbeat,
		Locale:    "en_US",
	})
}

// 定期检查连接状态
// - 如果连接断开，尝试重连
// - 如何自定义重连策略？
// 重连策略：
// - 默认重连检测：1s
// - 如果连接断开，则开始 1.5 倍指数级增长的重连检测，最长不超过 60s
// - 如果连接成功，则重置重连检测时间为 1s
// - 提供一个每次重试时的回调函数，用户可以在每次重试时做一些事情，比如打印日志
func (cli *Client) keepAlive() {
	// 指数退避策略
	expbf := backoff.NewExponentialBackOff()
	expbf.MaxElapsedTime = 0 // 无限重试
	expbf.MaxInterval = backoff.DefaultMaxInterval

	reconnect := func() error {
		newConn, err := newConnection(cli.source, cli.cfg)
		if err != nil {
			return err
		}
		cli.conn = newConn
		return nil
	}

	for {
		select {
		case <-cli.ctx.Done():
			cli.l.Printf("rabbitmq driver keep alive context done")
			return
		case <-time.After(cli.cfg.Heartbeat):
			if cli.conn.IsClosed() {
				cli.l.Printf("rabbitmq driver keep alive, reconnecting...")
				err := backoff.RetryNotify(reconnect, expbf, cli.cfg.RetryNotify)
				if err != nil {
					// 重连失败
					if cli.cfg.RetryNotify != nil {
						cli.cfg.RetryNotify(err, expbf.GetElapsedTime())
					}
				}
				expbf.Reset()
				continue
			}
			cli.l.Printf("rabbitmq driver keep alive, ok")
		}
	}
}

func (cli *Client) Close() error {
	return cli.conn.Close()
}
