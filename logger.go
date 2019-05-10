package consumer

// A Logger is a minimal interface to as a adaptor for external logging library to consumer
type Logger interface {
	Log(...interface{})
}
