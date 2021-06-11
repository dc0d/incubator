package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	stdlog "log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dc0d/incubator/service/mq"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func main() {
	// AWS_PROFILE should be set
	// AWS_REGION should be set
	// QUEUE_NAME should be set

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGABRT)
	defer cancel()

	opts := Options{
		Ctx:       ctx,
		AWSRegion: os.Getenv("AWS_REGION"),
		QueueName: os.Getenv("QUEUE_NAME"),
	}

	client := NewSQSClient(opts)

	for i := 0; i < 5; i++ {
		msg := fmt.Sprintf(`{
			"name": "task-%v",
			"data": {
				"size": 110,
				"text": "done"
			}
		}`, i)
		_, err := client.SendMessage(msg)
		if err != nil {
			lerr.Println(err)
		}
	}

	go func() {
		var opts mq.Options
		opts.Client = client.ReceiveMessage
		opts.Ctx = ctx
		opts.Handler = func(rcvd mq.Message) {
			defer func(start time.Time) {
				linf.Println("elapsed", time.Since(start))
			}(time.Now())

			msg := rcvd.(types.Message)

			if msg.Body == nil {
				lerr.Println("nil msg body")
				return
			}
			linf.Println(*msg.Body)

			_, err := client.DeleteMessage(*msg.ReceiptHandle)
			if err != nil {
				lerr.Println(err)
			}
		}
		opts.RespawnAfter.Count = 100
		opts.RespawnAfter.Elapsed = time.Minute * 5

		worker := mq.New(opts)
		worker.Start()
	}()

	<-ctx.Done()
}

type SQSClient struct {
	ctx context.Context

	client   *sqs.Client
	queueURL string
}

func NewSQSClient(opts Options) *SQSClient {
	opts.validate()

	res := &SQSClient{}
	res.ctx = opts.Ctx

	cfg, err := config.LoadDefaultConfig(res.ctx, config.WithRegion(opts.AWSRegion))
	if err != nil {
		panic(err)
	}

	res.client = sqs.NewFromConfig(cfg)

	name := new(string)
	*name = opts.QueueName

	input := &sqs.GetQueueUrlInput{
		QueueName: name,
	}
	result, err := res.client.GetQueueUrl(res.ctx, input)
	if err != nil {
		panic(err)
	}

	res.queueURL = *result.QueueUrl

	return res
}

func (obj *SQSClient) ReceiveMessage() (mq.ReceiverOutput, error) {
	input := &sqs.ReceiveMessageInput{}

	input.MaxNumberOfMessages = 10
	input.WaitTimeSeconds = 20
	input.VisibilityTimeout = 60 * 60

	queueURL := obj.queueURL
	input.QueueUrl = &queueURL

	output, err := obj.client.ReceiveMessage(obj.ctx, input)
	if err != nil {
		return nil, err
	}

	return &ReceiveMessageOutput{output: output}, nil
}

func (obj *SQSClient) SendMessage(msg interface{}) (*sqs.SendMessageOutput, error) {
	input := &sqs.SendMessageInput{}

	queueURL := obj.queueURL
	input.QueueUrl = &queueURL

	switch x := msg.(type) {
	case string:
		input.MessageBody = &x
	case *string:
		if x == nil {
			return nil, errors.New("msg is a nil string pointer")
		}

		body := *x
		input.MessageBody = &body
	default:
		content, err := json.Marshal(msg)
		if err != nil {
			return nil, err
		}

		body := string(content)
		input.MessageBody = &body
	}

	return obj.client.SendMessage(obj.ctx, input)
}

func (obj *SQSClient) DeleteMessage(receiptHandle string) (*sqs.DeleteMessageOutput, error) {
	input := &sqs.DeleteMessageInput{}

	queueURL := obj.queueURL
	input.QueueUrl = &queueURL

	input.ReceiptHandle = &receiptHandle

	return obj.client.DeleteMessage(obj.ctx, input)
}

type ReceiveMessageOutput struct {
	output *sqs.ReceiveMessageOutput
}

func (obj *ReceiveMessageOutput) Messages() (result []mq.Message) {
	for _, msg := range obj.output.Messages {
		result = append(result, msg)
	}

	return
}

type Options struct {
	Ctx       context.Context
	AWSRegion string
	QueueName string
}

func (obj Options) validate() {
	if obj.Ctx == nil {
		panic("Ctx must be provided.")
	}

	if obj.AWSRegion == "" {
		panic("AWSRegion must be provided.")
	}

	if obj.QueueName == "" {
		panic("QueueName must be provided.")
	}
}

var (
	logFlags = stdlog.Ltime | stdlog.Lshortfile | stdlog.Lmicroseconds | stdlog.Ldate

	linf = stdlog.New(os.Stdout, "[info ] ", logFlags)
	lerr = stdlog.New(os.Stdout, "[error] ", logFlags)
)
