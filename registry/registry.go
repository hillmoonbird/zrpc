package registry

import (
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

type ZRegistry struct {
	timeout time.Duration           // 服务过期时间
	mu      sync.Mutex              // 保证 servers 读写安全
	servers map[string]*ServerItem  // 服务列表
}

type ServerItem struct {
	Addr  string     // 服务地址
	start time.Time  // 上一次发送心跳时间
}

const (
	defaultPath    = "/_zrpc_/registry"
	defaultTimeout = time.Minute * 5
)

// 新建一个指定过期时间的注册中心实例
func New(timeout time.Duration) *ZRegistry {
	return &ZRegistry{
		servers: make(map[string]*ServerItem),
		timeout: timeout,
	}
}

var DefaultGeeRegister = New(defaultTimeout)

// 添加服务实例，如果服务已经存在，则更新 start
func (r *ZRegistry) putServer(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.servers[addr]
	if s == nil {
		// 若实例不存在，则新建一个服务实例
		r.servers[addr] = &ServerItem{Addr: addr, start: time.Now()}
	} else {
		// 若实例存在，则更新心跳时间
		s.start = time.Now() 
	}
}

// 返回可用的服务列表，如果存在超时的服务，则删除
func (r *ZRegistry) aliveServers() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var alive []string
	for addr, s := range r.servers {
		if r.timeout == 0 || s.start.Add(r.timeout).After(time.Now()) {
			alive = append(alive, addr)
		} else {
			delete(r.servers, addr)
		}
	}
	sort.Strings(alive)
	return alive
}

// ZRegistry 采用 HTTP 协议提供服务，且所有的有用信息都承载在 HTTP Header 中
// 运行在 /_zrpc_/registry 
func (r *ZRegistry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		// 返回所有可用的服务列表，通过自定义字段 X-Zrpc-Servers 承载
		w.Header().Set("X-Zrpc-Servers", strings.Join(r.aliveServers(), ","))
	case "POST":
		// 添加服务实例或发送心跳，通过自定义字段 X-Zrpc-Server 承载
		addr := req.Header.Get("X-Zrpc-Server")
		if addr == "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		r.putServer(addr)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// 为 registerPath 上的 ZRegistry 消息注册一个 HTTP handler
func (r *ZRegistry) HandleHTTP(registryPath string) {
	http.Handle(registryPath, r)
	log.Println("rpc registry path:", registryPath)
}

func HandleHTTP() {
	DefaultGeeRegister.HandleHTTP(defaultPath)
}

// 作为服务器的辅助函数，每过一段时间发送一条心跳消息
func Heartbeat(registry, addr string, duration time.Duration) {
	if duration == 0 {
		// 保证在被注册中心移除之前，有足够的时间发送心跳
		duration = defaultTimeout - time.Duration(1)*time.Minute
	}
	var err error
	err = sendHeartbeat(registry, addr)
	go func() {
		t := time.NewTicker(duration)
		for err == nil {
			<-t.C
			err = sendHeartbeat(registry, addr)
		}
	}()
}

// 发送心跳
func sendHeartbeat(registry, addr string) error {
	log.Println(addr, "send heart beat to registry", registry)
	httpClient := &http.Client{}
	req, _ := http.NewRequest("POST", registry, nil)
	req.Header.Set("X-Zrpc-Server", addr)
	if _, err := httpClient.Do(req); err != nil {
		log.Println("rpc server: heart beat err:", err)
		return err
	}
	return nil
}