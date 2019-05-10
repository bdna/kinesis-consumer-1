package internal

import (
	"log"
)

// noopLogger implements logger interface with discard
type NoopLogger struct {
	Logger *log.Logger
}

// Log using stdlib logger. See log.Println.
func (l NoopLogger) Log(args ...interface{}) {
	l.Logger.Println(args...)
}
