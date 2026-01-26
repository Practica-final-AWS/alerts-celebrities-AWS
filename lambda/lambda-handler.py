import boto3
import os
import json

rekognition = boto3.client('rekognition')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE_NAME'])
sns = boto3.client('sns')
sns_topic_arn = os.environ['SNS_TOPIC_ARN']

def handler(event, context):
    for record in event['Records']:
        # SQS body contains the S3 event
        s3_event = json.loads(record['body'])
        if 'Records' not in s3_event: continue
        
        for s3_rec in s3_event['Records']:
            bucket = s3_rec['s3']['bucket']['name']
            key = s3_rec['s3']['object']['key']
            
            # Call Rekognition
            response = rekognition.recognize_celebrities(
                Image={'S3Object': {'Bucket': bucket, 'Name': key}}
            )
            
            celebs = response.get('CelebrityFaces', [])
            is_celebrity = len(celebs) > 0
            celeb_name = str(celebs[0]['Name']) if is_celebrity else "None"

            # Save to DynamoDB
            table.put_item(Item={
                'ImageKey': key,
                'IsCelebrity': is_celebrity,
                'CelebrityName': celeb_name,
                'Bucket': bucket
            })

            # Publicar en SNS si hay un famoso
            if is_celebrity:
                message = f"Se detectó al famoso {celeb_name} en la imagen {key} del bucket {bucket}."
                sns.publish(
                    TopicArn=sns_topic_arn,
                    Message=message,
                    Subject="Alerta de Celebridad Detectada"
                )
    return {"status": "success"}
