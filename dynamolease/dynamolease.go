// Package dynamolease is a jobqueue Lease on an AWS DynamoDB table, taken with conditional writes. The table has a
// string partition key "name"; its "ttl" attribute (epoch seconds) may be set as the table's TTL attribute so
// DynamoDB removes old rows (that is cleanup only: expiry is checked on every write).
package dynamolease

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/ez-technologies/jobqueue"
)

// Lease keeps leases in Table.
type Lease struct {
	c     *dynamodb.Client
	table string
	now   func() time.Time
}

var _ jobqueue.Lease = (*Lease)(nil)

// New returns leases in table (default "jobqueue_leases").
func New(c *dynamodb.Client, table string) *Lease {
	if table == "" {
		table = "jobqueue_leases"
	}
	return &Lease{c: c, table: table, now: time.Now}
}

// CreateTable makes the table (on demand billing) and turns on its TTL; for setup scripts and tests.
func (l *Lease) CreateTable(ctx context.Context) error {
	_, err := l.c.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String(l.table),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("name"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("name"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	var inUse *types.ResourceInUseException
	if err != nil && !errors.As(err, &inUse) {
		return err
	}
	_, _ = l.c.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{TableName: aws.String(l.table),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{AttributeName: aws.String("ttl"), Enabled: aws.Bool(true)}})
	return nil
}

func ms(t time.Time) string { return strconv.FormatInt(t.UnixMilli(), 10) }

// Acquire writes the lease if it is free, expired or holder's.
func (l *Lease) Acquire(ctx context.Context, name, holder string, ttl time.Duration) (bool, error) {
	now := l.now()
	exp := now.Add(ttl)
	_, err := l.c.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(l.table),
		Item: map[string]types.AttributeValue{
			"name":       &types.AttributeValueMemberS{Value: name},
			"holder":     &types.AttributeValueMemberS{Value: holder},
			"expires_ms": &types.AttributeValueMemberN{Value: ms(exp)},
			"ttl":        &types.AttributeValueMemberN{Value: strconv.FormatInt(exp.Add(time.Hour).Unix(), 10)},
		},
		ConditionExpression:      aws.String("attribute_not_exists(#n) OR holder = :h OR expires_ms < :now"),
		ExpressionAttributeNames: map[string]string{"#n": "name"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":h": &types.AttributeValueMemberS{Value: holder}, ":now": &types.AttributeValueMemberN{Value: ms(now)}},
	})
	var failed *types.ConditionalCheckFailedException
	if errors.As(err, &failed) {
		return false, nil
	}
	return err == nil, err
}

// Release deletes the lease if holder has it.
func (l *Lease) Release(ctx context.Context, name, holder string) error {
	_, err := l.c.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName:                 aws.String(l.table),
		Key:                       map[string]types.AttributeValue{"name": &types.AttributeValueMemberS{Value: name}},
		ConditionExpression:       aws.String("holder = :h"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":h": &types.AttributeValueMemberS{Value: holder}},
	})
	var failed *types.ConditionalCheckFailedException
	if errors.As(err, &failed) {
		return nil
	}
	return err
}

// VersionStore keeps the newest version per key in the same table (items "version#<key>"), raised by a
// conditional update.
type VersionStore struct{ l *Lease }

// NewVersionStore returns versions in table (default "jobqueue_leases"; CreateTable makes it).
func NewVersionStore(c *dynamodb.Client, table string) *VersionStore {
	return &VersionStore{l: New(c, table)}
}

var _ jobqueue.VersionStore = (*VersionStore)(nil)

func (v *VersionStore) Observe(ctx context.Context, key string, version int64) error {
	_, err := v.l.c.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(v.l.table),
		Key:                       map[string]types.AttributeValue{"name": &types.AttributeValueMemberS{Value: "version#" + key}},
		UpdateExpression:          aws.String("SET v = :v"),
		ConditionExpression:       aws.String("attribute_not_exists(v) OR v < :v"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":v": &types.AttributeValueMemberN{Value: strconv.FormatInt(version, 10)}},
	})
	var failed *types.ConditionalCheckFailedException
	if errors.As(err, &failed) {
		return nil // a newer one is there
	}
	return err
}

func (v *VersionStore) Latest(ctx context.Context, key string) (int64, bool, error) {
	out, err := v.l.c.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(v.l.table), ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{"name": &types.AttributeValueMemberS{Value: "version#" + key}}})
	if err != nil {
		return 0, false, err
	}
	n, ok := out.Item["v"].(*types.AttributeValueMemberN)
	if !ok {
		return 0, false, nil
	}
	val, err := strconv.ParseInt(n.Value, 10, 64)
	return val, err == nil, err
}
