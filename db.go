package db

import (
	"context"
	"embed"
	"time"
)

type ClientOption struct {
	TTL time.Duration
	Dir string
	FS  *embed.FS
}

func NewClient(ctx context.Context, option ClientOption) (*Client, error) {
	return NewBaseClient[any](ctx, option)
}

type Client = BaseClient[any]
