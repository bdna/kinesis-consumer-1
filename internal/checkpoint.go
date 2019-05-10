package internal

// noopCheckpoint implements the checkpoint interface with discard
type NoopCheckpoint struct{}

func (n NoopCheckpoint) Set(string, string, string) error   { return nil }
func (n NoopCheckpoint) Get(string, string) (string, error) { return "", nil }
