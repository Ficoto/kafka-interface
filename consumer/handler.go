package consumer

import "github.com/Ficoto/go-gp"

type IsRetry func(msg any, failTimes int) bool

type Handler struct {
	Topic      string
	Key        string
	Handle     func(ctx Context) error
	PoolSize   int
	Callback   func(msg any, err error)
	RetryTimes int
	IsRetry    IsRetry
}

const DefaultRetryTimes = 3

func DefaultIsRetry(msg interface{}, failTimes int) bool {
	if failTimes >= DefaultRetryTimes {
		return false
	}
	return true
}

func IsRetryByFailTimes(ft int) IsRetry {
	return func(msg any, failTimes int) bool {
		if failTimes >= ft {
			return false
		}
		return true
	}
}

type RealHandler struct {
	Handle   func(msg any) error
	Callback func(msg any, err error)
	IsRetry  IsRetry
	GP       *gp.Pool
}
