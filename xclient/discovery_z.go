package xclient

import (
	"log"
	"net/http"
	"strings"
	"time"
)

type ZRegistryDiscovery struct {
	*MultiServersDiscovery
	registry   string         // 注册中心的地址
	timeout    time.Duration  // 服务列表的过期时间
	lastUpdate time.Time      // 最后从注册中心更新服务列表的时间
}

const defaultUpdateTimeout = time.Second * 10

// 更新服务列表
func (d *ZRegistryDiscovery) Update(servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.servers = servers
	d.lastUpdate = time.Now()
	return nil
}

// 超时重新获取服务列表
func (d *ZRegistryDiscovery) Refresh() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	// 若未超时
	if d.lastUpdate.Add(d.timeout).After(time.Now()) {
		return nil
	}
	log.Println("rpc registry: refresh servers from registry", d.registry)
	resp, err := http.Get(d.registry)
	if err != nil {
		log.Println("rpc registry refresh err:", err)
		return err
	}
	// 从 HTTP 响应报头的　X-Zrpc-Servers　字段获取服务列表
	servers := strings.Split(resp.Header.Get("X-Zrpc-Servers"), ",")
	d.servers = make([]string, 0, len(servers))
	for _, server := range servers {
		if strings.TrimSpace(server) != "" {
			d.servers = append(d.servers, strings.TrimSpace(server))
		}
	}
	d.lastUpdate = time.Now()
	return nil
}

func (d *ZRegistryDiscovery) Get(mode SelectMode) (string, error) {
	// 需要先调用 Refresh 确保服务列表没有过期
	if err := d.Refresh(); err != nil {
		return "", err
	}
	return d.MultiServersDiscovery.Get(mode)
}

func (d *ZRegistryDiscovery) GetAll() ([]string, error) {
	// 需要先调用 Refresh 确保服务列表没有过期
	if err := d.Refresh(); err != nil {
		return nil, err
	}
	return d.MultiServersDiscovery.GetAll()
}

func NewZRegistryDiscovery(registerAddr string, timeout time.Duration) *ZRegistryDiscovery {
	if timeout == 0 {
		timeout = defaultUpdateTimeout
	}
	d := &ZRegistryDiscovery{
		MultiServersDiscovery: NewMultiServerDiscovery(make([]string, 0)),
		registry:              registerAddr,
		timeout:               timeout,
	}
	return d
}