package encodedkv

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
)

func NewEncodedKV(bucket nats.KeyValue, codec Codec) nats.KeyValue {
	return &encodedKV{bucket, codec}
}

type Codec interface {
	EncodeKey(key string) (string, error)
	DecodeKey(key string) (string, error)
	EncodeRange(keys string) (string, error)
}

type encodedKV struct {
	bucket nats.KeyValue
	codec  Codec
}

type watcher struct {
	watcher nats.KeyWatcher
	codec   Codec
	updates chan nats.KeyValueEntry
	ctx     context.Context
	cancel  context.CancelFunc
}

type entry struct {
	codec Codec
	entry nats.KeyValueEntry
}

func (e *entry) Key() string {
	dk, err := e.codec.DecodeKey(e.entry.Key())
	if err != nil {
		return "" // TODO not good.
	}

	return dk
}

func (e *entry) Bucket() string             { return e.entry.Bucket() }
func (e *entry) Value() []byte              { return e.entry.Value() }
func (e *entry) Revision() uint64           { return e.entry.Revision() }
func (e *entry) Created() time.Time         { return e.entry.Created() }
func (e *entry) Delta() uint64              { return e.entry.Delta() }
func (e *entry) Operation() nats.KeyValueOp { return e.entry.Operation() }

func (w *watcher) Updates() <-chan nats.KeyValueEntry { return w.updates }
func (w *watcher) Stop() error {
	if w.cancel != nil {
		w.cancel()
	}

	return w.watcher.Stop()
}

func (e *encodedKV) newWatcher(w nats.KeyWatcher) nats.KeyWatcher {
	watch := &watcher{watcher: w, codec: e.codec, updates: make(chan nats.KeyValueEntry, 32)}
	watch.ctx, watch.cancel = context.WithCancel(context.Background())

	go func() {
		for {
			select {
			case ent := <-w.Updates():
				if ent == nil {
					watch.updates <- nil
					continue
				}

				watch.updates <- &entry{
					codec: e.codec,
					entry: ent,
				}
			case <-watch.ctx.Done():
				return
			}
		}
	}()

	return watch
}

func (e *encodedKV) Get(key string) (nats.KeyValueEntry, error) {
	ek, err := e.codec.EncodeKey(key)
	if err != nil {
		return nil, err
	}

	ent, err := e.bucket.Get(ek)
	if err != nil {
		return nil, err
	}

	return &entry{
		codec: e.codec,
		entry: ent,
	}, nil
}

func (e *encodedKV) Put(key string, value []byte) (revision uint64, err error) {
	ek, err := e.codec.EncodeKey(key)
	if err != nil {
		return 0, err
	}

	return e.bucket.Put(ek, value)
}

func (e *encodedKV) Create(key string, value []byte) (revision uint64, err error) {
	ek, err := e.codec.EncodeKey(key)
	if err != nil {
		return 0, err
	}

	return e.bucket.Create(ek, value)
}

func (e *encodedKV) Update(key string, value []byte, last uint64) (revision uint64, err error) {
	ek, err := e.codec.EncodeKey(key)
	if err != nil {
		return 0, err
	}

	return e.bucket.Update(ek, value, last)
}

func (e *encodedKV) Delete(key string) error {
	ek, err := e.codec.EncodeKey(key)
	if err != nil {
		return err
	}

	return e.bucket.Delete(ek)
}

func (e *encodedKV) Purge(key string) error {
	ek, err := e.codec.EncodeKey(key)
	if err != nil {
		return err
	}

	return e.bucket.Purge(ek)
}

func (e *encodedKV) Watch(keys string, opts ...nats.WatchOpt) (nats.KeyWatcher, error) {
	ek, err := e.codec.EncodeRange(keys)
	if err != nil {
		return nil, err
	}

	nw, err := e.bucket.Watch(ek, opts...)
	if err != nil {
		return nil, err
	}

	return e.newWatcher(nw), err
}

func (e *encodedKV) History(key string, opts ...nats.WatchOpt) ([]nats.KeyValueEntry, error) {
	ek, err := e.codec.EncodeKey(key)
	if err != nil {
		return nil, err
	}

	var res []nats.KeyValueEntry
	hist, err := e.bucket.History(ek, opts...)
	if err != nil {
		return nil, err
	}

	for _, ent := range hist {
		res = append(res, &entry{e.codec, ent})
	}

	return res, nil
}

func (e *encodedKV) PutString(key string, value string) (revision uint64, err error) {
	return e.Put(key, []byte(value))
}
func (e *encodedKV) WatchAll(opts ...nats.WatchOpt) (nats.KeyWatcher, error) {
	return e.bucket.WatchAll(opts...)
}
func (e *encodedKV) Keys(opts ...nats.WatchOpt) ([]string, error) { return e.bucket.Keys(opts...) }
func (e *encodedKV) Bucket() string                               { return e.bucket.Bucket() }
func (e *encodedKV) PurgeDeletes(opts ...nats.WatchOpt) error     { return e.bucket.PurgeDeletes(opts...) }
func (e *encodedKV) Status() (nats.KeyValueStatus, error)         { return e.bucket.Status() }
