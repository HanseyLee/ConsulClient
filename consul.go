package consul

import (
	"errors"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
)

// Consul TTL status refer to https://www.consul.io/docs/agent/checks.html
const (
	TTLFail   = "fail"
	TTLPass   = "pass"
	TTLWarn   = "warn"
	TTLUpdate = "update"
)

// one address one singleton
var clientSingletonMap = make(map[string]*api.Client)

var mu sync.Mutex

// Consul struct
type Consul struct {
	ClusterAddrs []string               `json:"clusteraddrs,omitempty"`
	Token        string                 `json:"token,omitempty"`
	Client       *api.Client            `json:"client,omitempty"`
	ServiceName  string                 `json:"servicename,omitempty"`
	Tags         []string               `json:"tags,omitempty"`
	Address      string                 `json:"address,omitempty"` // local service ip
	Port         int                    `json:"port,omitempty"`    // local service port
	ID           string                 `json:"id,omitempty"`      // local service port
	Check        *api.AgentServiceCheck `json:"checks,omitempty"`  // health check
}

// NewConsul new Consul with initiated client
func NewConsul(clusterAddrs []string, token string) (*Consul, error) {
	c := new(Consul)
	c.ClusterAddrs = clusterAddrs
	c.Token = token
	var err error
	c.Client, err = getClient(clusterAddrs, token)
	return c, err
}

// getClient one address, one client, mutliple tries, fast success
// different clusterAddrs(or same clusterAddrs but different order inside) may have different clients
func getClient(clusterAddrs []string, token string) (*api.Client, error) {
	for _, addr := range clusterAddrs {
		if client, ok := clientSingletonMap[addr]; ok {
			// double-check avaibility
			if _, err := client.Agent().NodeName(); err == nil {
				return client, nil
			}
		}
		if client, err := newClient(addr, token); err == nil && client != nil {
			mu.Lock()
			// double-check in lock
			if _, ok := clientSingletonMap[addr]; !ok {
				clientSingletonMap[addr] = client
			}
			mu.Unlock()
			return client, nil
		}
	}
	return nil, errors.New("invalid agent cluster addresses or token")
}

// newClient try to new a client
func newClient(addr, token string) (*api.Client, error) {
	cfg := api.DefaultConfig()
	cfg.Address = addr
	cfg.Token = token
	var client *api.Client
	var err error
	client, err = api.NewClient(cfg)
	if err == nil {
		if _, err = client.Agent().NodeName(); err != nil {
			client = nil
		}
	}
	return client, err
}

// DefaultTTLCheck 30s ttl and 3 days(72 hours) deregistration timeout
func DefaultTTLCheck() *api.AgentServiceCheck {
	ttl := time.Duration(30) * time.Second
	deregisterTimeout := time.Duration(72) * time.Hour
	return TTLCheck(ttl, deregisterTimeout)
}

// TTLCheck service report health state periodically
func TTLCheck(ttl, deregisterTimeout time.Duration) *api.AgentServiceCheck {
	check := &api.AgentServiceCheck{
		TTL:                            ttl.String(),
		DeregisterCriticalServiceAfter: deregisterTimeout.String(),
	}
	return check
}

// HTTPCheck consul cluster poll service periodically
func HTTPCheck(endpoint string, interval, deregisterTimeout time.Duration) *api.AgentServiceCheck {
	check := &api.AgentServiceCheck{
		HTTP:                           endpoint,
		Interval:                       interval.String(),
		DeregisterCriticalServiceAfter: deregisterTimeout.String(),
	}
	return check
}

// Register register a new service with the local agent
func (c *Consul) Register(ttlType bool, customizedHealthCheck func() (string, error)) error {
	err := c.register()
	if err == nil {
		if ttlType {
			if customizedHealthCheck == nil {
				go c.reportHealthState(DefaultHealthCheck)
			} else {
				go c.reportHealthState(customizedHealthCheck)
			}
		}
	}
	return err
}

// Deregister deregister a service by id with the local agent
func (c *Consul) Deregister() error {
	return c.Client.Agent().ServiceDeregister(c.ID)
}

// // CatalogRegister register a new service in global culster view, top-level
// func (c *Consul) CatalogRegister() {
// 	// catalog.register
// }

// // CatalogDeregister register a new service in global culster view, top-level
// func (c *Consul) CatalogDeregister() error {
// 	// catalog.deregister
// }

// DefaultHealthCheck simple response
func DefaultHealthCheck() (string, error) {
	now := time.Now().Format("2006-01-02 15:04:05")
	return now + " " + LocalIP() + ": I'm OK", nil
}

// register a new service with the local agent
func (c *Consul) register() error {
	agent := c.Client.Agent()
	reg := &api.AgentServiceRegistration{
		Name:    c.ServiceName,
		Tags:    c.Tags,
		Address: c.Address,
		Port:    c.Port,
		ID:      c.ID,
		Check:   c.Check,
	}
	return agent.ServiceRegister(reg)
}

// reportHealthState TTL/2 period
func (c *Consul) reportHealthState(customizedCheck func() (string, error)) {
	d, err := time.ParseDuration(c.Check.TTL)
	if err != nil {
		panic("TTL Check duration panic, exit program")
	}
	checkID := "service:" + c.ID
	c.Client.Agent().UpdateTTL(checkID, "initial health status report...", TTLPass)
	ticker := time.NewTicker(d / 2)
	for range ticker.C {
		if output, err := customizedCheck(); err == nil {
			c.Client.Agent().UpdateTTL(checkID, output, TTLPass)
		} else {
			c.Client.Agent().UpdateTTL(checkID, err.Error(), TTLFail)
		}
	}
}

// KVPut put kv to cluster kv-store
func (c *Consul) KVPut(k, v string) error {
	kv := c.Client.KV()
	// PUT a new KV pair
	p := &api.KVPair{Key: k, Value: []byte(v)}
	_, err := kv.Put(p, nil)
	return err
}

// KVGet get kv from cluster kv-store
func (c *Consul) KVGet(k string) (string, error) {
	kv := c.Client.KV()
	pair, _, err := kv.Get(k, nil)
	if err != nil {
		return "", err
	}
	return string(pair.Value), nil
}

// KVDelete delete kv
func (c *Consul) KVDelete(k string) error {
	kv := c.Client.KV()
	_, err := kv.Delete(k, nil)
	return err
}

// KVList list kv pairs: []*KVPair
func (c *Consul) KVList(prefix string) (api.KVPairs, error) {
	kv := c.Client.KV()
	pairs, _, err := kv.List(prefix, nil)
	return pairs, err
}

// DiscoverService discover service by name and tags, return map[serviceID]serviceAddr:servicePort
func (c *Consul) DiscoverService(service string, tags []string) (map[string]string, error) {
	catalog := c.Client.Catalog()
	m := make(map[string]string)
	services, _, err := catalog.ServiceMultipleTags(service, tags, nil)
	if err != nil {
		return nil, err
	}
	for _, s := range services {
		// s: api.CatalogService
		m[s.ServiceID] = s.ServiceAddress + ":" + strconv.Itoa(s.ServicePort)
	}
	return m, nil
}

// DiscoverAllServices return map[serviceName]tags
func (c *Consul) DiscoverAllServices() (map[string][]string, error) {
	catalog := c.Client.Catalog()
	m, _, err := catalog.Services(nil)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// LocalIP get local ip for
func LocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "localhost"
	}
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return "localhost"
}

// blockingCurrentGoroutine block routine for debug
func blockingCurrentGoroutine() {
	ch := make(chan int)
	<-ch
}
