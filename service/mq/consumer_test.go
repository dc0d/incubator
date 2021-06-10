//go:generate moq -out ./mock_client_test.go ./ Client

package mq

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

func Test_Options(t *testing.T) {
	t.Parallel()

	t.Run(`Handler is mandatory`, func(t *testing.T) {
		var sut Options

		sut.Ctx = context.TODO()
		sut.ClientFactory = func() Client { return &ClientMock{} }

		assert.PanicsWithValue(t,
			"Handler must be provided",
			func() { sut.validate() })
	})

	t.Run(`Client is mandatory`, func(t *testing.T) {
		var sut Options

		sut.Ctx = context.TODO()
		sut.Handler = func(msg Message) { panic("n/a") }

		assert.PanicsWithValue(t,
			"Client must be provided",
			func() { sut.validate() })
	})

	t.Run(`Ctx is mandatory`, func(t *testing.T) {
		var sut Options

		sut.Handler = func(msg Message) { panic("n/a") }
		sut.ClientFactory = func() Client { return &ClientMock{} }

		assert.PanicsWithValue(t,
			"Ctx must be provided",
			func() { sut.validate() })
	})
}

func Test_Consumer_should_call_handler_on_receiving_each_new_message(t *testing.T) {
	t.Parallel()

	var (
		worker *Consumer
		opts   Options
	)

	var (
		expectedMessage string
		actualMessage   string
	)

	expectedMessage = "EXPECTED PAYLOAD 1"

	opts.Ctx = context.TODO()
	opts.Handler = func(msg Message) { actualMessage = msg.(string) }

	client := &ClientMock{}
	client.ReceiveMessageFunc = func() (ReceiveMessageOutput, error) {
		var output receiveMessageOutputFunc = func() []Message { return []Message{expectedMessage} }
		return output, nil
	}
	opts.ClientFactory = func() Client { return client }

	worker = New(opts)
	worker.Start()

	diff := func() string { return cmp.Diff(expectedMessage, actualMessage) }
	assertExpectedMessageReceived := func() bool { return diff() == "" }
	assert.Eventuallyf(t,
		assertExpectedMessageReceived,
		time.Millisecond*1500,
		time.Millisecond*50,
		"diff: %v", diff())
}

func Test_Consumer_should_call_handler_for_each_message_concurrently(t *testing.T) {
	t.Parallel()

	var (
		worker *Consumer
		opts   Options
	)

	var (
		handlerCalls int64
	)

	const (
		numberOfMessagesInTheQueue = 3
		operationTimeout           = time.Millisecond * 100
	)

	handlerTriggered := make(chan struct{}, numberOfMessagesInTheQueue*10)

	opts.Ctx = context.TODO()
	opts.Handler = func(msg Message) {
		defer func() {
			select {
			case handlerTriggered <- struct{}{}:
			default:
			}
		}()

		time.Sleep(operationTimeout / 2)
		atomic.AddInt64(&handlerCalls, 1)
	}

	counter := 0
	client := &ClientMock{}
	client.ReceiveMessageFunc = func() (ReceiveMessageOutput, error) {
		var output receiveMessageOutputFunc = func() []Message {
			var result []Message

			if counter < numberOfMessagesInTheQueue {
				counter++
				result = append(result, fmt.Sprintf("EXPECTED PAYLOAD %v", counter))
			}

			return result
		}

		return output, nil
	}
	opts.ClientFactory = func() Client { return client }

	worker = New(opts)
	worker.Start()

	<-handlerTriggered

	assertHandlerCalledExpectedNumberOfTimes := func() bool {
		return atomic.LoadInt64(&handlerCalls) == numberOfMessagesInTheQueue
	}
	assert.Eventuallyf(t,
		assertHandlerCalledExpectedNumberOfTimes,
		operationTimeout,
		time.Millisecond*5,
		"%v %v", atomic.LoadInt64(&handlerCalls), numberOfMessagesInTheQueue)
}

func Test_Consumer_should_stop_when_the_context_is_canceled(t *testing.T) {
	t.Parallel()

	var (
		worker *Consumer
		opts   Options
	)

	ctx, cancel := context.WithCancel(context.Background())

	opts.Ctx = ctx
	opts.Handler = func(msg Message) {}

	client := &ClientMock{}
	client.ReceiveMessageFunc = func() (ReceiveMessageOutput, error) {
		return nil, nil
	}
	opts.ClientFactory = func() Client { return client }

	worker = New(opts)
	worker.Start()

	go time.AfterFunc(time.Millisecond*50, func() { cancel() })
	assert.Eventually(t,
		func() bool { return worker.isStopped(0) },
		time.Millisecond*150,
		time.Millisecond*5)
}

func Test_Consumer_should_continue_to_receive_after_error_when_receiving_message(t *testing.T) {
	t.Parallel()

	var (
		worker *Consumer
		opts   Options
	)

	var (
		expectedMessage string
		actualMessage   string
	)

	expectedMessage = "EXPECTED PAYLOAD 2"

	opts.Ctx = context.TODO()
	opts.Handler = func(msg Message) { actualMessage = msg.(string) }

	count := 0
	client := &ClientMock{}
	client.ReceiveMessageFunc = func() (ReceiveMessageOutput, error) {
		count++
		if count < 10 {
			return nil, errors.New("some error")
		}

		var output receiveMessageOutputFunc = func() []Message { return []Message{expectedMessage} }
		return output, nil
	}
	opts.ClientFactory = func() Client { return client }

	worker = New(opts)
	worker.Start()

	diff := func() string { return cmp.Diff(expectedMessage, actualMessage) }
	assertExpectedMessageReceived := func() bool { return diff() == "" }
	assert.Eventuallyf(t,
		assertExpectedMessageReceived,
		time.Millisecond*1500,
		time.Millisecond*50,
		"diff: %v", diff())
}

func Test_Consumer_should_continue_if_a_handler_panics(t *testing.T) {
	t.Parallel()

	var (
		worker *Consumer
		opts   Options
	)

	var (
		handlerCalls int64
	)

	opts.Ctx = context.TODO()
	opts.Handler = func(msg Message) {
		defer atomic.AddInt64(&handlerCalls, 1)
		if atomic.LoadInt64(&handlerCalls) == 1 {
			panic("panic on second call")
		}
	}

	count := 0
	client := &ClientMock{}
	client.ReceiveMessageFunc = func() (ReceiveMessageOutput, error) {
		if atomic.LoadInt64(&handlerCalls) >= 3 {
			select {}
		}

		count++

		var output receiveMessageOutputFunc = func() []Message {
			return []Message{
				fmt.Sprintf("MSG-%v", count),
			}
		}
		return output, nil
	}
	opts.ClientFactory = func() Client { return client }

	worker = New(opts)
	worker.Start()

	assert.Eventuallyf(t,
		func() bool { return atomic.LoadInt64(&handlerCalls) >= 3 },
		time.Millisecond*1500,
		time.Millisecond*50,
		"should call handler")
}

func Test_Consumer_should_respawn_after_specific_number_of_messages_handled(t *testing.T) {
	t.Parallel()

	var (
		worker *Consumer
		opts   Options
	)

	opts.Ctx = context.TODO()
	opts.Handler = func(msg Message) {}

	var messagesFromQueue int64
	client := &ClientMock{}
	client.ReceiveMessageFunc = func() (ReceiveMessageOutput, error) {
		defer func() { atomic.AddInt64(&messagesFromQueue, 1) }()
		if atomic.LoadInt64(&messagesFromQueue) > 20 {
			select {}
		}

		var output receiveMessageOutputFunc = func() []Message { return []Message{"a message"} }
		return output, nil
	}
	opts.ClientFactory = func() Client { return client }

	opts.RespawnAfter.Count = 10

	var loopExitCount int64
	worker = New(opts)
	worker.loopExited = func() { atomic.AddInt64(&loopExitCount, 1) }
	worker.Start()

	if !assert.Eventually(t,
		func() bool { return atomic.LoadInt64(&loopExitCount) == 2 },
		time.Millisecond*1500,
		time.Millisecond*50) {
		t.Logf("should respawn twice but respawned %v times", atomic.LoadInt64(&loopExitCount))
	}
}

func Test_Consumer_should_respawn_after_specific_time_has_elapsed(t *testing.T) {
	t.Parallel()

	var (
		worker *Consumer
		opts   Options
	)

	opts.Ctx = context.TODO()
	opts.Handler = func(msg Message) {}

	client := &ClientMock{}
	client.ReceiveMessageFunc = func() (ReceiveMessageOutput, error) {
		var output receiveMessageOutputFunc = func() []Message { return []Message{"a message"} }
		return output, nil
	}
	opts.ClientFactory = func() Client { return client }

	opts.RespawnAfter.Elapsed = time.Millisecond * 50

	var loopExitCount int64
	worker = New(opts)
	worker.loopExited = func() { atomic.AddInt64(&loopExitCount, 1) }
	worker.Start()

	assert.Eventuallyf(t,
		func() bool { return atomic.LoadInt64(&loopExitCount) >= 2 },
		time.Millisecond*300,
		time.Millisecond*10,
		"should respawn (at least) twice")
}

func Test_Consumer_should_not_start_new_handlers_when_the_number_of_in_flight_messages_passed_the_max(t *testing.T) {
	t.Parallel()

	var (
		worker *Consumer
		opts   Options
	)

	var (
		activeHandlers int64
	)

	const (
		maxInFlight = 4
	)

	handlerTriggered := make(chan struct{}, maxInFlight*10)
	opts.Ctx = context.TODO()
	opts.Handler = func(msg Message) {
		defer func() {
			select {
			case handlerTriggered <- struct{}{}:
			default:
			}
		}()

		defer atomic.AddInt64(&activeHandlers, -1)
		atomic.AddInt64(&activeHandlers, 1)
		time.Sleep(time.Millisecond * 50)
	}

	client := &ClientMock{}
	client.ReceiveMessageFunc = func() (ReceiveMessageOutput, error) {
		var output receiveMessageOutputFunc = func() []Message {
			var result []Message
			for i := 0; i < maxInFlight; i++ {
				result = append(result, fmt.Sprintf("MSG-%v", i))
			}

			return result
		}
		return output, nil
	}
	opts.ClientFactory = func() Client { return client }

	opts.MaxInFlight = maxInFlight

	worker = New(opts)
	worker.Start()

	<-handlerTriggered

	assert.Eventuallyf(t,
		func() bool {
			return atomic.LoadInt64(&activeHandlers) <= 2*maxInFlight
		},
		time.Millisecond*1500,
		time.Millisecond*30,
		"inflight count %v", atomic.LoadInt64(&activeHandlers))
}

type receiveMessageOutputFunc func() []Message

func (obj receiveMessageOutputFunc) Messages() []Message { return obj() }
