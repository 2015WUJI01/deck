package deck

import (
	"context"
)

type Consumer interface {
	Consume(ctx context.Context, queue string, handler func(source <-chan any)) error
	Stop()
	Close() error
}
