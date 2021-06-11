// Package mq contains constructs for working with
// message queues - like a consumer worker.
// Consumer is the consumer worker and it's purpose is
// to read the messages from the queue by calling the MessageReceiver
// and feed those messages to the MessageHandler. It maintains the
// message receiving loop.
//
// See the LICENSE file.
package mq
