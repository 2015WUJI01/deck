package deck

import (
	"context"
	"github.com/zeromicro/go-zero/core/fx"
)

type Consumer interface {
	Consume(ctx context.Context, queue string, handler func(stream fx.Stream)) error
	Close() error
}
