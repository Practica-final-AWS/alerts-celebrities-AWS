import logging

from datetime import datetime, timezone

from urllib.parse import unquote_plus


import boto3

from botocore.exceptions import ClientError


logger = logging.getLogger()

logger.setLevel(logging.INFO)


rek = boto3.client("rekognition")

ddb = boto3.resource("dynamodb")


DDB_TABLE = os.environ["DDB_TABLE"]

SOURCE_BUCKET = os.getenv("SOURCE_BUCKET", "s3-imagenes-practica-final")

TARGET_LABELS = [x.strip() for x in os.getenv("TARGET_LABELS", "Person").split(",") if x.strip()]

MIN_CONFIDENCE = float(os.getenv("MIN_CONFIDENCE", "80"))

SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN") # opcional


table = ddb.Table(DDB_TABLE)

sns = boto3.client("sns") if SNS_TOPIC_ARN else None



def _extract_s3_records_from_sqs_body(body_str: str):

"""

S3 -> SQS directo: body es JSON con {"Records":[...]}.

A veces hay envoltura SNS -> SQS: {"Message":"{...json...}"}.

Este helper soporta ambos.

"""

payload = json.loads(body_str)


# SNS envelope (si existiera)

if isinstance(payload, dict) and isinstance(payload.get("Message"), str):

try:

payload = json.loads(payload["Message"])

except json.JSONDecodeError:

pass


if isinstance(payload, dict) and isinstance(payload.get("Records"), list):

return payload["Records"]


return []



def _process_image(bucket: str, key: str, request_id: str):

# Normaliza key (S3 suele venir URL-encoded)

key = unquote_plus(key)


# (1) "Tomar" imagen referenciándola desde S3 para Rekognition

# No necesitas descargarla: Rekognition acepta S3Object.

resp = rek.detect_labels(

Image={"S3Object": {"Bucket": bucket, "Name": key}},

MinConfidence=MIN_CONFIDENCE,

)


labels = resp.get("Labels", [])

matched_labels = [

{"name": l["Name"], "confidence": float(l["Confidence"])}

for l in labels

if l.get("Name") in TARGET_LABELS and float(l.get("Confidence", 0)) >= MIN_CONFIDENCE

]

is_match = len(matched_labels) > 0


# (3) Imprimir o enviar mensaje

summary = (

f"[{'COINCIDE' if is_match else 'NO COINCIDE'}] "

f"{bucket}/{key} | objetivos={TARGET_LABELS} | "

f"match={matched_labels if matched_labels else 'ninguno'}"

)

logger.info(summary)


if sns:

sns.publish(

TopicArn=SNS_TOPIC_ARN,

Subject="Resultado detección (Rekognition)",

Message=summary,

)


# (4) Guardar en DynamoDB

now = datetime.now(timezone.utc).isoformat()

item = {

"imageKey": f"{bucket}/{key}", # PK

"ts": now, # SK

"bucket": bucket,

"key": key,

"matched": is_match,

"targetLabels": TARGET_LABELS,

"minConfidence": MIN_CONFIDENCE,

"matchedLabels": matched_labels,

"allLabels": [{"name": l["Name"], "confidence": float(l["Confidence"])} for l in labels],

"lambdaRequestId": request_id,

}


table.put_item(Item=item)



def lambda_handler(event, context):

"""

Trigger: SQS

Soporta partial batch response para que SQS reintente solo los mensajes fallidos.

"""

batch_failures = []

request_id = getattr(context, "aws_request_id", "unknown")


for sqs_record in event.get("Records", []):

msg_id = sqs_record.get("messageId", "")

try:

s3_records = _extract_s3_records_from_sqs_body(sqs_record["body"])

if not s3_records:

raise ValueError("No se encontraron Records de S3 dentro del body del mensaje SQS.")


for s3rec in s3_records:

bucket = s3rec["s3"]["bucket"]["name"]

key = s3rec["s3"]["object"]["key"]


# Validación opcional del bucket esperado

if bucket != SOURCE_BUCKET:

logger.warning(

"Bucket inesperado: %s (esperado: %s). Procesando igualmente.",

bucket, SOURCE_BUCKET

)


_process_image(bucket, key, request_id)


except (KeyError, ValueError, ClientError) as e:

logger.exception("Error procesando mensaje SQS %s: %s", msg_id, e)

if msg_id:

batch_failures.append({"itemIdentifier": msg_id})

except Exception as e:

logger.exception("Error inesperado procesando mensaje SQS %s: %s", msg_id, e)

if msg_id:

batch_failures.append({"itemIdentifier": msg_id})


return {"batchItemFailures": batch_failures}