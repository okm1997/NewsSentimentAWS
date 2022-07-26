import requests
import json
import datetime
import boto3
import requests
import base64
from botocore.exceptions import ClientError


def get_secret():

    secret_name = "DemoAPIKey"
    region_name = "ap-south-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            decoded_binary_secret = base64.b64decode(
                get_secret_value_response['SecretBinary'])

    return get_secret_value_response


def lambda_handler(event, context):
    # TODO implement
    print("button pressed")
    print(event)
    print("just changing a print")
    if event['action'] == 'insert news':
        findNews()
    elif event['action'] == 'delete news':
        deleteNews()

    return 'End of News Sentiment function'


def deleteNews():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('news')

    response = table.scan()
    if 'Items' in response:
        items = response['Items']
        for row in items:
            sentiment = row['sentiment']
            timestamp = row['timestamp']
            delresponse = table.delete_item(
                Key={
                    'sentiment': sentiment,
                    'timestamp': timestamp
                }
            )


def findNews():
    api = get_secret()['SecretString']
    pr = json.loads(api)
    key = pr['APIKey']
    urlwithkey = "https://newsapi.org/v2/top-headlines?country=in&apiKey="+key
    response = requests.get(urlwithkey)

    d = response.json()
    if (d['status']) == 'ok':
        for article in d['articles']:
            print(article['title'])
            newsTitle = article['title']
            timestamp = article['publishedAt']
            sentiment = json.loads(getSentiment(newsTitle))
            print(sentiment['Sentiment'])
            insertDynamo(sentiment['Sentiment'], newsTitle, timestamp)


def getSentiment(newsTitle):
    comprehend = boto3.client(service_name='comprehend')
    return(json.dumps(comprehend.detect_sentiment(Text=newsTitle, LanguageCode='en'), sort_keys=True))


def insertDynamo(sentiment, newsTitle, timestamp):
    print("inside insert dynamo function")
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('news')
    response = table.put_item(
        Item={
            'sentiment': sentiment,
            'title': newsTitle,
            'timestamp': timestamp
        }
    )
