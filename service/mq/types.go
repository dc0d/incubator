package mq

type (
	MessageHandler func(msg Message)

	// MessageReceiver is the MQ client that returns batches of messages.
	MessageReceiver func() (ReceiverOutput, error)
)

type (
	ReceiverOutput = interface {
		Messages() []Message
	}

	Message = interface{}
)
