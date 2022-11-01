package zrpc

import (
	"encoding/json"
	"io"
	"log"
	"net"
	"reflect"
	"sync"
	"strings"
	"errors"
	"time"
	"fmt"
	"net/http"

	"zrpc/codec"
)

const MagicNumber = 0x3bef5c

type Option struct {
	MagicNumber int              // MagicNumber 标识这是一个 zrpc 请求
	CodecType   codec.Type       // 客户端可以选择不同 Codec 来解码 body
	ConnectTimeout time.Duration // 客户端创建连接限时，默认值为 10s
	HandleTimeout  time.Duration // 值为 0 时表示没有时间限制
}

var DefaultOption = &Option{
	MagicNumber: MagicNumber,
	CodecType:   codec.GobType,
	ConnectTimeout: time.Second * 10,
}

// Server 表示一个 RPC 服务器
type Server struct{
	serviceMap sync.Map
}

// 服务器新建函数
func NewServer() *Server {
	return &Server{}
}

// *Server 的默认实例
var DefaultServer = NewServer()

// 在单一连接上运行服务。为连接提供服务期间，ServeConn 阻塞，直到客户端挂断。
func (server *Server) ServeConn(conn io.ReadWriteCloser) {
	defer func() {
		_ = conn.Close()
	}()
	var opt Option
	// 将 conn 接收的下一个 JSON 编码的值反序列化后写入 opt 
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error", err)
		return
	}
	// 若非 zrpc 请求
	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc server: invalid magic number %x", opt.MagicNumber)
		return
	}
	// 获取 codec 构造函数
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
		return
	}
	server.serveCodec(f(conn), &opt)
}

// 发生错位时响应参数的一个占位符
var invalidRequest = struct{}{}

// 读取、处理并回复请求
func (server *Server) serveCodec(cc codec.Codec, opt *Option) {
	sending := new(sync.Mutex)  // 确保发送的是完整响应
	wg := new(sync.WaitGroup)   // 等待所有请求处理完毕
	for {
		// 读取请求
		req, err := server.readRequest(cc)
		if err != nil {
			if req == nil {
				// 只有在 header 解析失败时，才终止循环
				break
			}
			req.h.Error = err.Error()
			// 回复请求（通过锁 sending 保证串行）
			server.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		wg.Add(1)
		// 处理请求（通过协程并发执行）
		go server.handleRequest(cc, req, sending, wg, opt.HandleTimeout)
	}
	wg.Wait()
	_ = cc.Close()
}

// 存储请求信息
type request struct {
	h             *codec.Header //请求头
	argv, replyv  reflect.Value //请求参数与返回值
	mtype         *methodType   //请求方法类型
	svc           *service	    //请求服务
}

func (server *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}
	return &h, nil
}

// 通过 ServiceMethod 从 serviceMap 中找到对应的 service
func (server *Server) findService(serviceMethod string) (svc *service, mtype *methodType, err error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc server: service/method request ill-formed: " + serviceMethod)
		return
	}
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	// 在 serviceMap 中找到对应的 service 实例
	svci, ok := server.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: can't find service " + serviceName)
		return
	}
	// 从 service 实例的 method 中，找到对应的 methodType
	svc = svci.(*service)
	mtype = svc.method[methodName]
	if mtype == nil {
		err = errors.New("rpc server: can't find method " + methodName)
	}
	return
}

func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	h, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	req := &request{h: h}
	req.svc, req.mtype, err = server.findService(h.ServiceMethod)
	if err != nil {
		return req, err
	}
	// 创建两个入参实例
	req.argv = req.mtype.newArgv()
	req.replyv = req.mtype.newReplyv()
	// 确保 argvi 是一个指针，因为 ReadBody 需要指针作为参数
	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		argvi = req.argv.Addr().Interface()
	}
	// 将请求报文反序列化为第一个入参 argv
	if err = cc.ReadBody(argvi); err != nil {
		log.Println("rpc server: read body err:", err)
		return req, err
	}
	return req, nil
}

func (server *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

// 请求已注册的 rpc 方法，来获取正确返回值
func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()
	called := make(chan struct{})
	sent := make(chan struct{})
	go func() {
		err := req.svc.call(req.mtype, req.argv, req.replyv)
		called <- struct{}{}
		if err != nil {
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			sent <- struct{}{}
			return
		}
		server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
		sent <- struct{}{}
	}()
	if timeout == 0 {
		<-called
		<-sent
		return
	}
	select {
	// time.After() 先于 called 接收到消息，说明处理已经超时，called 和 sent 都将被阻塞
	case <-time.After(timeout):
		req.h.Error = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
		server.sendResponse(cc, req.h, invalidRequest, sending)
	// called 信道接收到消息，代表处理没有超时，继续执行 sendResponse
	case <-called:
		<-sent
	}
}

// 监听端接收连接，并为每个传入连接的请求提供服务
func (server *Server) Accept(lis net.Listener) {
	// for 循环等待 socket 连接建立，并开启子协程处理，处理过程交给了 ServeConn 方法
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Println("rpc server: accept error:", err)
			return
		}
		go server.ServeConn(conn)
	}
}
  
// 直接供外部调用的 API
func Accept(lis net.Listener) { 
	DefaultServer.Accept(lis) 
}

// 在服务端中发布新方法
func (server *Server) Register(rcvr interface{}) error {
	s := newService(rcvr)
	if _, dup := server.serviceMap.LoadOrStore(s.name, s); dup {
		return errors.New("rpc: service already defined: " + s.name)
	}
	return nil
}

// 在 DefaultServer 中发布接收方的方法。
func Register(rcvr interface{}) error { return DefaultServer.Register(rcvr) }

const (
	connected        = "200 Connected to zRPC"
	defaultRPCPath   = "/_zprc_"
	defaultDebugPath = "/debug/zrpc"
)

// 实现了一个 http.Handler，以回应 RPC 请求
func (server *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "405 must CONNECT\n")
		return
	}
	// 调用 Hijack() 后，http server 标准库不会再对连接做任何处理，转而由调用者管理及关闭连接
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking ", req.RemoteAddr, ": ", err.Error())
		return
	}
	_, _ = io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
	server.ServeConn(conn)
}

// 在 rpcPath 上为 RPC 消息注册一个 HTTP 处理器
// 并在 debugPath 上注册一个 debugging 处理器
// 通常在 go 语句中，仍然需要调用 http.Serve()
func (server *Server) HandleHTTP() {
	http.Handle(defaultRPCPath, server)
	http.Handle(defaultDebugPath, debugHTTP{server})
	log.Println("rpc server debug path:", defaultDebugPath)
}
 
// 默认服务器注册 HTTP 处理器
func HandleHTTP() {
	DefaultServer.HandleHTTP()
}