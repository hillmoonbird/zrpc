package zrpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
	"context"
	"net/http"
	"bufio"
	"strings"

	"zrpc/codec"
)

// Call 表示一个有效的 RPC，承载 RPC 调用所需要的信息。
type Call struct {
	Seq           uint64      // 请求编号
	ServiceMethod string      // 服务与方法名称，格式 "<service>.<method>"
	Args          interface{} // 函数所需参数
	Reply         interface{} // 函数返回值
	Error         error       // 发生错误时，设置该值
	Done          chan *Call  // 支持异步调用
}

// 请求结束后，通知调用方
func (call *Call) done() {
	call.Done <- call
}

// Client 表示一个 RPC 客户端。一个客户端可能启动多个调用，也可能被多个协程所同时使用。
type Client struct {
	cc       codec.Codec       // 消息的编解码器
	opt      *Option
	sending  sync.Mutex        // 保证请求有序发送，防止多个请求报文混淆
	header   codec.Header
	mu       sync.Mutex        // 保证本结构体的读写安全
	seq      uint64            // 每个请求的唯一编号
	pending  map[uint64]*Call  // 存储未处理完的请求编号，键是编号，值是 Call 实例
	closing  bool              // 用户主动关闭客户端
	shutdown bool              // 运行出现错误，导致客户端不可用
}

var _ io.Closer = (*Client)(nil)

var ErrShutdown = errors.New("connection is shut down")

// 关闭客户端连接
func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing {
		return ErrShutdown
	}
	client.closing = true
	return client.cc.Close()
}

// 判断客户端是否可用
func (client *Client) IsAvailable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.shutdown && !client.closing
}

// 将参数 call 添加到 client.pending 中，并更新 client.seq
func (client *Client) registerCall(call *Call) (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing || client.shutdown {
		return 0, ErrShutdown
	}
	call.Seq = client.seq
	client.pending[call.Seq] = call
	client.seq++
	return call.Seq, nil
}

// 根据 seq，从 client.pending 中移除对应的 call，并返回
func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

// 服务端或客户端发生错误时调用，将 shutdown 设置为 true，且将错误信息通知所有 pending 状态的 call
func (client *Client) terminateCalls(err error) {
	client.sending.Lock()
	defer client.sending.Unlock()
	client.mu.Lock()
	defer client.mu.Unlock()
	client.shutdown = true
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}
}

// 客户端接收响应
func (client *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		if err = client.cc.ReadHeader(&h); err != nil {
			break
		}

		call := client.removeCall(h.Seq)
		switch {
		// call 不存在，可能是请求没有发送完整，或者因为其他原因被取消，但是服务端仍旧处理了
		case call == nil:
			err = client.cc.ReadBody(nil)
		// call 存在，但服务端处理出错
		case h.Error != "":
			call.Error = fmt.Errorf(h.Error)
			err = client.cc.ReadBody(nil)
			call.done()
		// call 存在，服务端处理正常，那么需要从 body 中读取 Reply 的值
		default:
			err = client.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body " + err.Error())
			}
			call.done()
		}
	}
	client.terminateCalls(err)
}

// 创建 Client 实例
func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	// 指定编解码函数
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client: codec error:", err)
		return nil, err
	}
	// 向服务器发送 Option 信息
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: options error: ", err)
		_ = conn.Close()
		return nil, err
	}
	return newClientCodec(f(conn), opt), nil
}

// 协商消息的编解码方式
func newClientCodec(cc codec.Codec, opt *Option) *Client {
	client := &Client{
		seq:     1,    // seq 从 1 开始, 0 表示无效调用
		cc:      cc,   
		opt:     opt,  
		pending: make(map[uint64]*Call),
	}
	// 创建一个子协程调用 receive() 接收响应
	go client.receive()
	return client
}

// 对传入的 Option 做语法分析
func parseOptions(opts ...*Option) (*Option, error) {
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}
	opt := opts[0]
	opt.MagicNumber = DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = DefaultOption.CodecType
	}
	return opt, nil
}

// 客户端向服务端发送请求
func (client *Client) send(call *Call) {
	// 确保客户端发送一个完整请求
	client.sending.Lock()
	defer client.sending.Unlock()
	// 注册本次调用
	seq, err := client.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}
	// 准备请求头
	client.header.ServiceMethod = call.ServiceMethod
	client.header.Seq = seq
	client.header.Error = ""
	// 编码并发送请求
	if err := client.cc.Write(&client.header, call.Args); err != nil {
		call := client.removeCall(seq)
		// call may be nil, it usually means that Write partially failed,
		// client has received the response and handled
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

// 客户端暴露给用户的 RPC 服务调用接口（异步），返回 call 实例
func (client *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	// 确保 done 是有缓存通道
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel is unbuffered")
	}
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	client.send(call)
	return call
}

// 调用 client.Go，并等待其完成
func (client *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	call := client.Go(serviceMethod, args, reply, make(chan *Call, 1))
	select {
	// 达到 context.WithTimeout 设置的超时时间
	case <-ctx.Done():
		client.removeCall(call.Seq)
		return errors.New("rpc client: call failed: " + ctx.Err().Error())
	// 调用结束
	case call := <-call.Done:
		return call.Error
	}
}

type clientResult struct {
	client *Client
	err    error
}

type newClientFunc func(conn net.Conn, opt *Option) (client *Client, err error)

func dialTimeout(f newClientFunc, network, address string, opts ...*Option) (client *Client, err error) {
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	// 如果连接创建超时，将返回错误
	// 此处连接创建主要包括：1. 解析参数 network 和 address 的值
	// 2. 创建 socket 实例并建立网络连接
	conn, err := net.DialTimeout(network, address, opt.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	// 若客户端为 nil，则关闭连接
	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()  
	// 使用子协程执行 NewClient，执行完成后则通过通道 ch 发送结果
	ch := make(chan clientResult)
	go func() {
		client, err := f(conn, opt)
		ch <- clientResult{client: client, err: err}
	}()
	// 若连接创建不限时，则直接返回创建结果
	if opt.ConnectTimeout == 0 {
		result := <-ch
		return result.client, result.err
	}
	select {
	// 如果 time.After() 信道先接收到消息，则说明 NewClient 执行超时，返回错误
	case <-time.After(opt.ConnectTimeout):
		return nil, fmt.Errorf("rpc client: connect timeout: expect within %s", opt.ConnectTimeout)
	case result := <-ch:
		return result.client, result.err
	}
}

// 连接到一个位于指定网络地址的 RPC 服务器
func Dial(network, address string, opts ...*Option) (*Client, error) {
	return dialTimeout(NewClient, network, address, opts...)
}

// 通过 HTTP 新建一个客户端实例作为传输协议
func NewHTTPClient(conn net.Conn, opt *Option) (*Client, error) {
	_, _ = io.WriteString(conn, fmt.Sprintf("CONNECT %s HTTP/1.0\n\n", defaultRPCPath))
	// 成功接收 HTTP 响应之后，才能切换到 RPC 协议
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err == nil && resp.Status == connected {
		return NewClient(conn, opt)
	}
	if err == nil {
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}
	return nil, err
}

// 连接到特定网络地址的一个 HTTP-RPC 服务器
func DialHTTP(network, address string, opts ...*Option) (*Client, error) {
	return dialTimeout(NewHTTPClient, network, address, opts...)
}

// 根据 rpcAddr 调用不同的函数来连接到一个 RPC 服务器
func XDial(rpcAddr string, opts ...*Option) (*Client, error) {
	parts := strings.Split(rpcAddr, "@")
	if len(parts) != 2 {
		return nil, fmt.Errorf("rpc client err: wrong format '%s', expect protocol@addr", rpcAddr)
	}
	protocol, addr := parts[0], parts[1]
	switch protocol {
	case "http":
		// http 协议
		return DialHTTP("tcp", addr, opts...)
	default:
		// tcp, unix 或其他传输协议
		return Dial(protocol, addr, opts...)
	}
}