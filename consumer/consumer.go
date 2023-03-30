package consumer

type Consumer interface {
	AddHandler(handlers ...Handler)
	Run()
	Close()
}
