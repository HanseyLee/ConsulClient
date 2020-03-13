package consul

import (
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
)

// Consul TTL status refer to https://www.consul.io/docs/agent/checks.html
const (
	TTLFail = "fail"
	TTLPass = "pass"
	TTLWarn = "warn"
)

var (
	// one address one singleton
	clientSingletonMap = make(map[string]*api.Client)

	mu sync.Mutex

	// HealthChanCap ...
	HealthChanCap = 10

	// HealthChan global
	HealthChan = make(chan CheckPoint, HealthChanCap)
)

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

// CheckPoint check runtime health point
type CheckPoint struct {
	App      string `json:"App,omitempty"`      // *Optional. App name
	Reciever string `json:"Reciever,omitempty"` // *Optional. Reciever struct name if function is method
	Function string `json:"Function,omitempty"` // *Optional. Function (or method) name
	Name     string `json:"Name"`               // *Required. self-defined check point name
	Status   bool   `json:"Status"`             // *Required. true is ok, false indicates exception etc.
	Info     string `json:"Info"`               // *Required. info or error message
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
			checkID := "service:" + c.ID
			c.Client.Agent().UpdateTTL(checkID, "initial health status report", TTLPass)
			go c.urgentHealthReport()
			if customizedHealthCheck == nil {
				go c.routineHealthReport(DefaultHealthCheck)
			} else {
				go c.routineHealthReport(customizedHealthCheck)
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

// DefaultHealthCheck simple response
func DefaultHealthCheck() (string, error) {
	now := time.Now().Format("2006-01-02 15:04:05")
	return now + " " + LocalIP() + ": I'm OK", nil
}

// urgentHealthReport start HealthChan channel to recieve urgent message and report
// continuous repeated fail will always trigger watch alarm (alarm threshold is "fail")
// skip healthy check point
func (c *Consul) urgentHealthReport() {
	for {
		cp := <-HealthChan
		if !cp.Status {
			checkID := "service:" + c.ID
			// make a status change, to ensure always trigger watch alarm
			c.Client.Agent().UpdateTTL(checkID, "", TTLWarn)
			if r, err := json.Marshal(cp); err == nil {
				c.Client.Agent().UpdateTTL(checkID, string(r), TTLFail)
			} else {
				c.Client.Agent().UpdateTTL(checkID, cp.Info, TTLFail)
			}
			time.Sleep(time.Second * 1)
		}
	}
}

// routineHealthReport report customizedCheck func result in fixed period (TTL/2)
// continuous repeated fail with same err string body will be triggered only once
// continuous repeated fail with different err string body will be triggered multiple times
func (c *Consul) routineHealthReport(customizedCheck func() (string, error)) {
	d, err := time.ParseDuration(c.Check.TTL)
	if err != nil {
		panic("TTL Check duration panic, exit program")
	}
	lastStatus := true
	lastErrMsg := ""
	checkID := "service:" + c.ID
	ticker := time.NewTicker(d / 2)
	for range ticker.C {
		if output, err := customizedCheck(); err == nil {
			c.Client.Agent().UpdateTTL(checkID, output, TTLPass)
			lastStatus = true
		} else {
			errMsg := err.Error()
			if !lastStatus && lastErrMsg != errMsg {
				// make a status change, to ensure trigger watch alarm
				c.Client.Agent().UpdateTTL(checkID, "", TTLWarn)
			}
			c.Client.Agent().UpdateTTL(checkID, errMsg, TTLFail)
			lastStatus = false
			lastErrMsg = errMsg
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
