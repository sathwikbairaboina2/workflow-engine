package core

import "sync"

// notifier wakes long-pollers. wait returns the current channel for a key; broadcast closes it and
// installs a fresh one, so a poller that grabbed the channel before checking for work never misses a wake-up.
type notifier struct {
	mu sync.Mutex
	ch map[string]chan struct{}
}

func newNotifier() *notifier { return &notifier{ch: map[string]chan struct{}{}} }

func (n *notifier) wait(key string) <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	c, ok := n.ch[key]
	if !ok {
		c = make(chan struct{})
		n.ch[key] = c
	}
	return c
}

func (n *notifier) broadcast(key string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if c, ok := n.ch[key]; ok {
		close(c)
		delete(n.ch, key)
	}
}

func wfKey(queue string) string  { return "workflow/" + queue }
func actKey(queue string) string { return "activity/" + queue }
