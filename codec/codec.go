package codec

import "io"

// 保存请求和响应中除参数和返回值以外的信息
type Header struct {
	ServiceMethod string //服务名和方法名，通常与 Go 中的结构体和方法相映射
	Seq           uint64 //请求的序号，也可以认为是某个请求的 ID，用来区分不同的请求
	Error         string //错误信息，客户端置为空，服务端如果如果发生错误，将错误信息置于 Error 中
}

// 抽象出对消息体进行编解码的接口 Codec，以实现不同的 Codec 实例
type Codec interface {
	io.Closer  //io.closer 接口定义了 close 方法，该方法用于关闭连接
	ReadHeader(*Header) error
	ReadBody(interface{}) error
	Write(*Header, interface{}) error
}

// 抽象出 Codec 的构造函数
type NewCodecFunc func(io.ReadWriteCloser) Codec

type Type string

const (
	GobType  Type = "application/gob"
	JsonType Type = "application/json" // 未使用
)

// 客户端和服务端可以通过 Codec 的 Type 得到构造函数，从而创建 Codec 实例
var NewCodecFuncMap map[Type]NewCodecFunc

func init() {
	NewCodecFuncMap = make(map[Type]NewCodecFunc)
	NewCodecFuncMap[GobType] = NewGobCodec
}