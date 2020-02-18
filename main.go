package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/sns"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

const (
	apiURL  = "https://api2.realtor.ca/Listing.svc/PropertySearch_Post"
	baseURL = "https://realtor.ca"

	dynamoPartitionKeyName  = "partition_key"
	dynamoPartitionKeyValue = "seen-listings"
)

var (
	payload         url.Values
	awsRegion       string
	awsAccountId    string
	dynamoTableName string
	snsTopicName    string
)

func init() {
	payload = url.Values{
		"ZoomLevel":            {"13"},
		"LatitudeMax":          {"43.51949"},
		"LongitudeMax":         {"-80.43042"},
		"LatitudeMin":          {"43.42644"},
		"LongitudeMin":         {"-80.66406"},
		"Sort":                 {"6-D"},
		"PropertyTypeGroupID":  {"1"},
		"PropertySearchTypeId": {"1"},
		"TransactionTypeId":    {"2"},
		"PriceMin":             {"539000"},
		"PriceMax":             {"701000"},
		"BedRange":             {"3-0"},
		"BathRange":            {"2-0"},
		"BuildingTypeId":       {"1"},
		"ConstructionStyleId":  {"3"},
		"Currency":             {"CAD"},
		"RecordsPerPage":       {"20"},
		"ApplicationId":        {"1"},
		"CultureId":            {"1"},
		"Version":              {"7.0"},
		"CurrentPage":          {""},
	}

	awsRegion = requiredEnvVar("AWS_REGION")
	awsAccountId = requiredEnvVar("AWS_ACCOUNT_ID")
	dynamoTableName = requiredEnvVar("DYNAMO_TABLE_NAME")
	snsTopicName = requiredEnvVar("SNS_TOPIC_NAME")
}

func requiredEnvVar(key string) string {
	ret := os.Getenv(key)
	if ret == "" {
		panic("Required environment variable not set: " + key)
	}
	return ret
}

type Listing struct {
	ID                 string
	RelativeDetailsURL string
}

func (l Listing) URL() string {
	return baseURL + l.RelativeDetailsURL
}

type Listings struct {
	Results []Listing
}

type ListingCache struct {
	PartitionKey string   `dynamodbav:"partition_key"`
	SeenIDs      []string `dynamodbav:"seen_ids"`
}

type DB struct {
	dynamo *dynamodb.DynamoDB
	cache  *ListingCache
}

func NewDB(session *session.Session) *DB {
	return &DB{dynamo: dynamodb.New(session)}
}

func (db *DB) Seen(ctx context.Context, listing Listing) (bool, error) {
	if db.cache == nil {
		if err := db.refreshCache(ctx); err != nil {
			return false, err
		}
	}
	for _, seenId := range db.cache.SeenIDs {
		if listing.ID == seenId {
			return true, nil
		}
	}
	return false, nil
}

func (db *DB) refreshCache(ctx context.Context) error {
	item, err := db.dynamo.GetItemWithContext(
		ctx,
		&dynamodb.GetItemInput{
			Key: map[string]*dynamodb.AttributeValue{
				dynamoPartitionKeyName: {S: aws.String(dynamoPartitionKeyValue)}},
			TableName: aws.String(dynamoTableName),
		})
	if err != nil {
		return err
	}
	if err = dynamodbattribute.UnmarshalMap(item.Item, &db.cache); err != nil {
		return err
	}

	return nil
}

func (db *DB) MarkSeen(ctx context.Context, listing Listing) error {
	_ = ctx
	if db.cache == nil {
		return errors.New("cache is not populated yet")
	}
	db.cache.SeenIDs = append(db.cache.SeenIDs, listing.ID)
	return nil
}

func (db *DB) Flush(ctx context.Context) error {
	// Set the partition key in case of empty cache
	db.cache.PartitionKey = dynamoPartitionKeyValue

	item, err := dynamodbattribute.MarshalMap(db.cache)
	if err != nil {
		return err
	}

	_, err = db.dynamo.PutItemWithContext(ctx, &dynamodb.PutItemInput{
		Item:         item,
		ReturnValues: aws.String(dynamodb.ReturnValueNone),
		TableName:    aws.String(dynamoTableName),
	})
	return err
}

type Notifier struct {
	sns      *sns.SNS
	topicArn *string
}

func NewNotifier(sess *session.Session) *Notifier {
	return &Notifier{
		sns:      sns.New(sess),
		topicArn: aws.String("arn:aws:sns:" + *sess.Config.Region + ":" + awsAccountId + ":" + snsTopicName),
	}
}

func (n *Notifier) SendListingAlert(ctx context.Context, listing Listing) error {
	_, err := n.sns.PublishWithContext(ctx, &sns.PublishInput{
		Message:  aws.String(n.formatMessage(listing)),
		Subject:  aws.String(n.formatSubject(listing)),
		TopicArn: n.topicArn,
	})
	return err
}

func (n *Notifier) formatMessage(listing Listing) string {
	return listing.URL()
}

func (n *Notifier) formatSubject(listing Listing) string {
	_ = listing
	return "New listing on Realtor.ca"
}

func HandleRequest(ctx context.Context) error {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region: aws.String(awsRegion),
		},
		SharedConfigState: session.SharedConfigEnable,
	}))

	listings, err := fetchListings(ctx)
	if err != nil {
		return err
	}

	db := NewDB(sess)
	defer func() {
		if err = db.Flush(ctx); err != nil {
			log.Println("Failed to flush cache to database")
			log.Fatal(err)
		}
	}()

	notify := NewNotifier(sess)

	for _, listing := range listings.Results {
		seen, err := db.Seen(ctx, listing)
		if err != nil {
			return err
		}

		if !seen {
			if err = notify.SendListingAlert(ctx, listing); err != nil {
				return err
			}

			_ = db.MarkSeen(ctx, listing)
		}
	}

	return nil
}

func fetchListings(ctx context.Context) (*Listings, error) {
	listings := &Listings{}

	req, _ := http.NewRequestWithContext(ctx, "POST", apiURL, strings.NewReader(payload.Encode()))
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return listings, err
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return listings, err
	}

	if err = json.Unmarshal(body, listings); err != nil {
		return listings, err
	}

	return listings, nil
}

func main() {
	lambda.Start(HandleRequest)
}
