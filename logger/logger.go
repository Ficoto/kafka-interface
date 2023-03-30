package logger

// LogWriter log writer interface
type LogWriter interface {
	Println(values ...any)
	Printf(string, ...any)
}

type NopLogger struct{}

func (NopLogger) Println(values ...any) {}

func (NopLogger) Printf(string, ...any) {}
