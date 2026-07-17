package db

import (
	"bytes"
	"context"
	"embed"
	"encoding/gob"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/gospider007/tools"
)

func NewBaseClient[T any](ctx context.Context, option ClientOption) (*BaseClient[T], error) {
	if ctx == nil {
		ctx = context.TODO()
	}
	context, cancel := context.WithCancel(ctx)
	client := &BaseClient[T]{
		ttl: option.TTL,
		ctx: context,
		cnl: cancel,
		dir: option.Dir,
		fs:  option.FS,
	}
	if client.dir != "" {
		if !tools.PathExist(client.dir) {
			err := os.MkdirAll(client.dir, 0777)
			if err != nil {
				return nil, err
			}
		}
	} else {
		go client.run()
	}
	return client, nil
}

type rawBaseData[T any] struct {
	data T
	time time.Time
	ttl  time.Duration
}
type BaseClient[T any] struct {
	ttl  time.Duration
	data sync.Map
	ctx  context.Context
	cnl  context.CancelFunc
	dir  string
	fs   *embed.FS
}

func (obj *BaseClient[T]) run() {
	interval := time.Second * 30
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-obj.ctx.Done():
			return
		default:
			obj.data.Range(func(key, value any) bool {
				val := value.(rawBaseData[T])
				if val.ttl > 0 && time.Since(val.time) > val.ttl {
					obj.data.Delete(key)
				}
				return true
			})
			timer.Reset(interval)
			select {
			case <-obj.ctx.Done():
				return
			case <-timer.C:
			}
		}
	}
}

func (obj *BaseClient[T]) set(key string, data T, ttls ...time.Duration) {
	var ttl time.Duration
	if len(ttls) > 0 {
		ttl = ttls[0]
	}
	obj.data.Store(key, rawBaseData[T]{
		data: data,
		time: time.Now(),
		ttl:  ttl,
	})
}

func (obj *BaseClient[T]) get(key string) (T, bool) {
	val, ok := obj.data.Load(key)
	if !ok {
		return *new(T), false
	}
	return val.(rawBaseData[T]).data, true
}
func (obj *BaseClient[T]) Set(key string, data T, ttls ...time.Duration) error {
	key = tools.Hex(tools.Md5(key))
	b := bytes.NewBuffer(nil)
	err := gob.NewEncoder(b).Encode(data)
	if err != nil {
		return err
	}
	if obj.dir != "" {
		return os.WriteFile(tools.PathJoin(obj.dir, key), b.Bytes(), 0777)
	} else {
		obj.set(key, data, ttls...)
	}
	return nil
}

func (obj *BaseClient[T]) Get(key string, t T) (bool, error) {
	key = tools.Hex(tools.Md5(key))
	if obj.fs != nil {
		b, err := obj.fs.ReadFile(obj.dir + "/" + key)
		if err != nil {
			return false, err
		}
		err = gob.NewDecoder(bytes.NewBuffer(b)).Decode(t)
		return true, err
	} else if obj.dir != "" {
		b, err := os.ReadFile(tools.PathJoin(obj.dir, key))
		if err != nil {
			return false, err
		}
		err = gob.NewDecoder(bytes.NewBuffer(b)).Decode(t)
		return true, err
	} else {
		b, ok := obj.get(key)
		if !ok {
			return ok, nil
		}
		rv := reflect.ValueOf(t)
		rv.Elem().Set(reflect.ValueOf(b))
		return ok, nil
	}
}
func (obj *BaseClient[T]) GetRaw(key string) (T, bool, error) {
	key = tools.Hex(tools.Md5(key))
	var data T
	if obj.fs != nil {
		b, err := obj.fs.ReadFile(obj.dir + "/" + key)
		if err != nil {
			return data, false, err
		}
		err = gob.NewDecoder(bytes.NewBuffer(b)).Decode(&data)
		return data, true, err
	} else if obj.dir != "" {
		b, err := os.ReadFile(tools.PathJoin(obj.dir, key))
		if err != nil {
			return data, false, err
		}
		err = gob.NewDecoder(bytes.NewBuffer(b)).Decode(&data)
		return data, true, err
	} else {
		b, ok := obj.get(key)
		return b, ok, nil
	}
}
func (obj *BaseClient[T]) Close() {
	obj.cnl()
	obj.data.Clear()
}
