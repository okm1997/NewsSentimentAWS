import boto3
from boto3.dynamodb.conditions import Key
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):

    table = dynamodb.Table('news')
    inputSentiment = event['sentiment']
    try:
        # Querying the table using Primary key
        response = table.query(
            KeyConditionExpression=Key('sentiment').eq(inputSentiment),
            Limit=10,
            ScanIndexForward=False)  # descending order of timestamp
        return response
    except:
        raise
