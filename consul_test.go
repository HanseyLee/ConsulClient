package consul

import (
	"testing"
)

const (
    CONSUL_TOKEN = "xxx"
)

func TestConsul(t *testing.T) {
	addrs := []string{"server:8500", "server3:8500", "server2:8500"}
	c, _ := NewConsul(addrs, CONSUL_TOKEN)

	// KVPut KVGet KVDelete KVList..
	key := "DEBUG/" + "test-consul"
	value := "test-consul"
	c.KVPut(key, value)
	wantV, _ := c.KVGet(key)
	if wantV != value {
		t.Errorf("kv get key error")
	}
	// service discovery
	c.DiscoverService("serviceA", nil)

	addrs2 := []string{"server:8500", "server3:8500", "server2:8500"}
	c2, _ := NewConsul(addrs2, CONSUL_TOKEN)
	key2 := "DEBUG/" + "test-consul2"
	value2 := "test-consul2"
	c2.KVPut(key2, value2)
	wantV2, _ := c2.KVGet(key2)
	if wantV2 != value2 {
		t.Errorf("kv get key error")
	}

	addrs3 := []string{"server3:8500", "server:8500", "server2:8500"}
	c3, _ := NewConsul(addrs3, CONSUL_TOKEN)
	key3 := "DEBUG/" + "test-consul3"
	value3 := "test-consul3"
	c2.KVPut(key3, value3)
	wantV3, _ := c3.KVGet(key3)
	if wantV3 != value3 {
		t.Errorf("kv get key error")
	}
}
