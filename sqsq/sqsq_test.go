package sqsq_test

import (
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/ez-technologies/jobqueue"
	"github.com/ez-technologies/jobqueue/jobqueuetest"
	"github.com/ez-technologies/jobqueue/memq"
	"github.com/ez-technologies/jobqueue/sqsq"
)

// client talks to the local ElasticMQ (JOBQUEUE_TEST_SQS_ENDPOINT) with placeholder keys; never to AWS.
func client(t *testing.T) *sqs.Client {
	t.Helper()
	ep := os.Getenv("JOBQUEUE_TEST_SQS_ENDPOINT")
	if ep == "" {
		t.Skip("JOBQUEUE_TEST_SQS_ENDPOINT is not set (eval \"$(scripts/test-env.sh up)\")")
	}
	return sqs.New(sqs.Options{Region: "us-east-1", BaseEndpoint: aws.String(ep),
		Credentials: credentials.NewStaticCredentialsProvider("local", "local", "")})
}

func newDriver(t *testing.T) *sqsq.Driver {
	return sqsq.New(client(t), sqsq.Config{Prefix: "jqtest", Wait: time.Second, CreateQueues: true})
}

func TestDriver(t *testing.T) {
	jobqueuetest.Driver(t, func(t *testing.T) jobqueue.Driver { return newDriver(t) })
}

func TestSchedulerOver(t *testing.T) {
	jobqueuetest.SchedulerOver(t, newDriver(t), memq.NewStore(), memq.NewLease())
}
