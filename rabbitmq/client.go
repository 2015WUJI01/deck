package rabbitmq

import (
	"context"
	"github.com/2015wuji01/deck/logger"
	"github.com/cenkalti/backoff/v4"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type ClientConfig struct {
	// 日志输出
	Logger logger.Interface

	// 重连检测的时间间隔，默认 1s
	Heartbeat time.Duration
	// 重连失败时的回调函数，可以在这里打印日志
	//RetryNotify backoff.Notify
}

type Client struct {
	source string
	ctx    context.Context
	cfg    ClientConfig
	conn   *amqp.Connection

	l logger.Interface
}

func NewClient(source string, cfg ClientConfig) (*Client, error) {
	var cli = &Client{
		source: source,
		ctx:    context.Background(),
		cfg:    cfg,
		l:      logger.Default,
	}
	if cfg.Logger != nil {
		cli.l = cfg.Logger
	}

	// 设置 amqp 底层的日志
	amqp.SetLogger(wrapAmqplogger(cli.l))

	var err error
	cli.conn, err = newConnection(source, cfg)
	if err != nil {
		cli.l.Error(cli.ctx, "failed to connect to rabbitmq: %v", err)
		return nil, err
	}
	cli.l.Info(cli.ctx, "rabbitmq connection established")

	go cli.keepAlive()
	return cli, nil
}

type amqplogger struct {
	logger.Interface
}

func (l *amqplogger) Printf(format string, v ...interface{}) {
	l.Trace(context.Background(), format, v...)
}

func wrapAmqplogger(l logger.Interface) amqp.Logging {
	return &amqplogger{l}
}

func newConnection(source string, cfg ClientConfig) (conn *amqp.Connection, err error) {
	cli, err := amqp.DialConfig(source, amqp.Config{
		Heartbeat: cfg.Heartbeat,
		Locale:    "en_US",
	})
	if err != nil {
		return nil, err
	}
	return cli, nil
}

func (cli *Client) keepAlive() {
	// 指数退避策略
	expbf := backoff.NewExponentialBackOff()
	expbf.MaxElapsedTime = 0 // 无限重试

	// 监听
	shutdown := cli.conn.NotifyClose(make(chan *amqp.Error))
	e := <-shutdown
	if e == nil {
		cli.l.Info(cli.ctx, "rabbitmq connection closed safely")
		return
	}
	cli.l.Error(cli.ctx, "rabbitmq connection closed unexpectedly: %v", e.Error())

	// 重连
	expbf.Reset()
	err := backoff.RetryNotify(cli.reconnect, expbf, nil)
	if err == nil {
		cli.l.Warn(cli.ctx, "rabbitmq reconnected, cost %v", expbf.GetElapsedTime().Round(10*time.Millisecond).String())
	}
}

func (cli *Client) reconnect() error {
	newConn, err := newConnection(cli.source, cli.cfg)
	if err != nil {
		return err
	}
	cli.conn = newConn
	go cli.keepAlive()
	return nil
}

func (cli *Client) Close() error {
	return cli.conn.Close()
}
