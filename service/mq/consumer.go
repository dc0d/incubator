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
	maxInFlight   int
	respawnAfter  struct {
		Count   int
		Elapsed time.Duration
	}

	activeLoop sync.WaitGroup
	loopExited func()

	throttler *throttler
}

func New(opts Options) *Consumer {
	opts.validate()

	res := &Consumer{
		ctx:           opts.Ctx,
		clientFactory: opts.ClientFactory,
		handler:       opts.Handler,
		respawnAfter:  opts.RespawnAfter,
		maxInFlight:   opts.MaxInFlight,
	}

	if res.maxInFlight > 0 {
		res.throttler = newThrottler(res.maxInFlight)
	}

	return res
}

func (obj *Consumer) Start() {
	obj.activeLoop.Add(1)
	go obj.loop()
}

func (obj *Consumer) loop() {
	defer obj.activeLoop.Done()
	if obj.loopExited != nil {
		defer obj.loopExited()
	}

	handledMessageCount := 0
	var respawnTimeout <-chan time.Time
	if obj.respawnAfter.Elapsed > 0 {
		respawnTimeout = time.After(obj.respawnAfter.Elapsed)
	}

	for {
		obj.throttleInFlightCount()

		if obj.shouldStop() {
			return
		}

		if obj.shouldRespawn(handledMessageCount, respawnTimeout) {
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

		if countRcvd == 0 {
			continue
		}

		if obj.respawnAfter.Count > 0 {
			handledMessageCount += countRcvd
		}

		for _, msg := range rcvd {
			obj.inFlightAdded()
			go func(msg Message) {
				defer obj.inFlightDone()
				defer func() { _ = recover() }()

				obj.handler(msg)
			}(msg)
		}
	}
}

func (obj *Consumer) throttleInFlightCount() {
	if obj.throttler == nil {
		return
	}

	obj.throttler.throttle()
}

func (obj *Consumer) inFlightDone() {
	if obj.throttler == nil {
		return
	}

	obj.throttler.dec()
}

func (obj *Consumer) inFlightAdded() {
	if obj.throttler == nil {
		return
	}

	obj.throttler.inc()
}

func (obj *Consumer) shouldRespawn(
	handledMessageCount int,
	respawnTimeout <-chan time.Time) bool {
	if obj.respawnAfter.Count > 0 &&
		obj.respawnAfter.Count <= handledMessageCount {
		return true
	} else {
		select {
		case <-respawnTimeout:
			return true
		default:
		}
	}

	return false
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
		obj.activeLoop.Wait()
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

type throttler struct {
	count int
	cond  *sync.Cond
	max   int
}

func newThrottler(max int) *throttler {
	return &throttler{
		cond: sync.NewCond(&sync.Mutex{}),
		max:  max,
	}
}

func (obj *throttler) inc() {
	obj.cond.L.Lock()
	defer obj.cond.L.Unlock()

	obj.count++
}

func (obj *throttler) dec() {
	defer obj.cond.Broadcast()

	obj.cond.L.Lock()
	defer obj.cond.L.Unlock()

	obj.count--
}

func (obj *throttler) throttle() {
	obj.cond.L.Lock()
	defer obj.cond.L.Unlock()

	for obj.count > obj.max {
		obj.cond.Wait()
	}
}

type Options struct {
	Ctx           context.Context
	ClientFactory ClientFactory
	Handler       MessageHandler
	MaxInFlight   int
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
