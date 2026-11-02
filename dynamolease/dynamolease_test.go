package dynamolease_test

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/ez-technologies/jobqueue"
	"github.com/ez-technologies/jobqueue/dynamolease"
	"github.com/ez-technologies/jobqueue/jobqueuetest"
)

// TestLease runs against DynamoDB Local (JOBQUEUE_TEST_DYNAMODB_ENDPOINT) with placeholder keys; never AWS.
func TestLease(t *testing.T) {
	ep := os.Getenv("JOBQUEUE_TEST_DYNAMODB_ENDPOINT")
	if ep == "" {
		t.Skip("JOBQUEUE_TEST_DYNAMODB_ENDPOINT is not set (eval \"$(scripts/test-env.sh up)\")")
	}
	c := dynamodb.New(dynamodb.Options{Region: "us-east-1", BaseEndpoint: aws.String(ep),
		Credentials: credentials.NewStaticCredentialsProvider("local", "local", "")})
	l := dynamolease.New(c, "jqtest_leases")
	if err := l.CreateTable(context.Background()); err != nil {
		t.Fatal(err)
	}
	jobqueuetest.Lease(t, func(t *testing.T) jobqueue.Lease { return l })
	jobqueuetest.Versions(t, func(t *testing.T) jobqueue.VersionStore { return dynamolease.NewVersionStore(c, "jqtest_leases") })
}
