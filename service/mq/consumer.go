package mq

import (
	"context"
	"sync"
	"time"
)

// Consumer is a consumer worker that processes the messages that it receives.
type Consumer struct {
	ctx           context.Context
	clientFactory ClientFactory
	handler       MessageHandler
	respawnAfter  struct {
		Count   int
		Elapsed time.Duration
	}

	activeGoroutines sync.WaitGroup
	loopExited       func()
}

func New(opts Options) *Consumer {
	opts.validate()

	res := &Consumer{
		ctx:           opts.Ctx,
		clientFactory: opts.ClientFactory,
		handler:       opts.Handler,
		respawnAfter:  opts.RespawnAfter,
	}

	return res
}

func (obj *Consumer) Start() {
	obj.activeGoroutines.Add(1)
	go obj.loop()
}

func (obj *Consumer) loop() {
	defer obj.activeGoroutines.Done()
	if obj.loopExited != nil {
		defer obj.loopExited()
	}

	handledMessageCount := 0
	var respawnTimeout <-chan time.Time
	if obj.respawnAfter.Elapsed > 0 {
		respawnTimeout = time.After(obj.respawnAfter.Elapsed)
	}

	for {
		if obj.shouldStop() {
			return
		}

		shouldRespawn := false
		if obj.respawnAfter.Count > 0 &&
			obj.respawnAfter.Count <= handledMessageCount {
			shouldRespawn = true
		} else {
			select {
			case <-respawnTimeout:
				shouldRespawn = true
			default:
			}
		}

		if shouldRespawn {
			obj.Start()
			return
		}

		output, err := obj.clientFactory().ReceiveMessage()
		if err != nil {
			continue
		}

		if output == nil {
			continue
		}

		rcvd := output.Messages()
		countRcvd := len(rcvd)

		if obj.respawnAfter.Count > 0 &&
			countRcvd > 0 {
			handledMessageCount += countRcvd
		}

		for _, msg := range rcvd {
			obj.activeGoroutines.Add(1)
			go func(msg Message) {
				defer obj.activeGoroutines.Done()
				defer func() { _ = recover() }()

				obj.handler(msg)
			}(msg)
		}
	}
}

func (obj *Consumer) shouldStop() bool {
	select {
	case <-obj.ctx.Done():
		return true
	default:
	}
	return false
}

func (obj *Consumer) isStopped(waitFor time.Duration) bool {
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		obj.activeGoroutines.Wait()
	}()

	const waitDelay = time.Millisecond * 300
	if waitFor <= 0 {
		waitFor = waitDelay
	}

	select {
	case <-stopped:
		return true
	case <-time.After(waitFor):
		return false
	}
}

type Options struct {
	Handler       MessageHandler
	ClientFactory ClientFactory
	Ctx           context.Context
	RespawnAfter  struct {
		Count   int
		Elapsed time.Duration
	}
}

func (obj *Options) validate() {
	if obj.Handler == nil {
		panic("Handler must be provided")
	}

	if obj.ClientFactory == nil {
		panic("Client must be provided")
	}

	if obj.Ctx == nil {
		panic("Ctx must be provided")
	}
}
