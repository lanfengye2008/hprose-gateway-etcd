/*
* @Author:<zibin_5257@163.com>
* @Date: 2019-04-10 13:40
 */
package main

import (
	"context"
	"errors"
	"github.com/vlorc/hprose-gateway-etcd/manager"
	"github.com/vlorc/hprose-gateway-etcd/resolver"
	types "github.com/vlorc/hprose-gateway-types"
	"go.etcd.io/etcd/clientv3"
	"sync"
)

type watcher struct {
	cli  func() *clientv3.Client
	data map[string]string
	lock sync.RWMutex
}

func NewWatcher(cli func() *clientv3.Client) *watcher {
	return &watcher{
		cli:  cli,
		data: make(map[string]string),
	}
}

func (t *watcher) Push(u []types.Update) (err error) {
	for _, v := range u {
		switch v.Op.String() {
		case "PUT":
			go t.setDataKey(v.Id, v.Value)
		case "DELETE":
			go t.delDataKey(v.Id)
		}
	}
	return nil
}

func (t *watcher) Pop() ([]types.Update, error) {
	return nil, nil
}

func (t *watcher) Close() (err error) {
	return
}

func (t *watcher) setDataKey(key, val string) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.data[key] = string(val)
}

func (t *watcher) delDataKey(key string) {
	t.lock.Lock()
	defer t.lock.Unlock()
	delete(t.data, key)
}

func (t *watcher) GetDatas() map[string]string {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.data
}

func (t *watcher) GetKey(id string) (value string, err error) {
	t.lock.RUnlock()
	defer t.lock.RUnlock()
	value, ok := t.data[id]
	if !ok {
		return "", errors.New("Key does not exist.")
	}
	return
}

//服务发现示例
func serviceDiscover() {
	var cli = func() *clientv3.Client {
		c, _ := clientv3.New(clientv3.Config{})
		return c
	}
	var ctx context.Context
	r := resolver.NewResolver(cli, ctx, "rpc" /*prefix*/)
	// print event
	w := NewWatcher(cli)
	go r.Watch("*", w)
}

//服务注册示例
func reg() {
	var cli = func() *clientv3.Client {
		c, _ := clientv3.New(clientv3.Config{})
		return c
	}
	manage := manager.NewManager(cli, context.Background(), "rpc" /*prefix*/, 5 /*ttl*/)
	s := manage.Register("user" /*name*/, "1" /*uuid*/)
	s.Update(&types.Service{
		Id:   "1",
		Name: "user",
		Url:  "http://localhost:8080",
	})
}
