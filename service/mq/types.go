package mq

type (
	MessageHandler func(msg Message)
	ClientFactory  func() Client
)

type Client interface {
	// ReceiveMessage returns batches of messages.
	ReceiveMessage() (ReceiveMessageOutput, error)
}

type (
	ReceiveMessageOutput = interface {
		Messages() []Message
	}

	Message = interface{}
)