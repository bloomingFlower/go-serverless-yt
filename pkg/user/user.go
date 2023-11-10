package user

import (
	"encoding/json"
	"errors"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/bloomingFlower/go-serverless-yt/pkg/validators"
)

var ErrorMethodNotAllowed = errors.New("Method not allowed")

// User is a struct that represents a user in the system
var (
	ErrorFailedToUnmarshalRecord = errors.New("Failed to unmarshal record")
	ErrorFailedToFetchRecord     = errors.New("Failed to fetch record")
	ErrorInvalidEmail            = errors.New("Invalid email")
	ErrorInvalidUserData         = errors.New("Invalid user data")
	ErrorCouldNotMarshalItem     = errors.New("Could not marshal item")
	ErrorCouldNotDeleteItem      = errors.New("Could not delete item")
	ErrorCouldNotDynamoPutItem   = errors.New("Could not dynamo put item")
	ErrorUserAlreadyExists       = errors.New("User already exists")
	ErrorUserDoesNotExist        = errors.New("User does not exist")
)

type User struct {
	Email     string `json:"email"`
	FirstName string `json:"firstName"`
	LastName  string `json:"lastName"`
}

func FetchUser(email, tableName string, dynaClient dynamodbiface.DynamoDBAPI) (*User, error) {
	input := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"email": {
				S: aws.String(email),
			},
		},
		TableName: aws.String(tableName),
	}
	result, err := dynaClient.GetItem(input)
	if err != nil {
		return nil, ErrorFailedToFetchRecord
	}

	item := new(User)
	err = dynamodbattribute.UnmarshalMap(result.Item, item)
	if err != nil {
		return nil, ErrorFailedToFetchRecord
	}
	return item, nil
}

func FetchUsers(tableName string, dynaClient dynamodbiface.DynamoDBAPI) (*[]User, error) {
	input := &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	}
	result, err := dynaClient.Scan(input)
	if err != nil {
		return nil, ErrorFailedToFetchRecord
	}

	items := new([]User)
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, items)
	if err != nil {
		return nil, ErrorFailedToFetchRecord
	}
	return items, nil
}

func CreateUser(req events.APIGatewayProxyRequest, tableName string, dynaClient dynamodbiface.DynamoDBAPI) (*User, error) {
	var u User
	if err := json.Unmarshal([]byte(req.Body), &u); err != nil {
		return nil, ErrorInvalidUserData
	}
	if !validators.IsEmailValid(u.Email) {
		return nil, ErrorInvalidEmail
	}
	currentUser, _ := FetchUser(u.Email, tableName, dynaClient)
	if currentUser != nil && len(currentUser.Email) > 0 {
		return nil, ErrorUserAlreadyExists
	}
	av, err := dynamodbattribute.MarshalMap(u)
	if err != nil {
		return nil, ErrorCouldNotMarshalItem
	}

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(tableName),
	}

	_, err = dynaClient.PutItem(input)
	if err != nil {
		return nil, ErrorCouldNotDynamoPutItem
	}
	return &u, nil
}

func UpdateUser(req events.APIGatewayProxyRequest, tableName string, dynaClient dynamodbiface.DynamoDBAPI) (*User, error) {
	var u User
	if err := json.Unmarshal([]byte(req.Body), &u); err != nil {
		return nil, ErrorInvalidUserData
	}
	currentUser, _ := FetchUser(u.Email, tableName, dynaClient)
	if currentUser == nil || len(currentUser.Email) == 0 {
		return nil, ErrorUserDoesNotExist
	}
	av, err := dynamodbattribute.MarshalMap(u)
	if err != nil {
		return nil, ErrorCouldNotMarshalItem
	}
	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(tableName),
	}
	_, err = dynaClient.PutItem(input)
	if err != nil {
		return nil, ErrorCouldNotDynamoPutItem
	}
	return &u, nil
}

func DeleteUser(req events.APIGatewayProxyRequest, tableName string, dynaClient dynamodbiface.DynamoDBAPI) error {
	email := req.PathParameters["email"]
	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"email": {
				S: aws.String(email),
			},
		},
		TableName: aws.String(tableName),
	}
	_, err := dynaClient.DeleteItem(input)
	if err != nil {
		return ErrorCouldNotDeleteItem
	}
	return nil
}
