import json, boto3, os, time

s3 = boto3.client("s3")
BUCKET = os.environ["BUCKET_NAME"]
ALLOWED = ["image/png","image/jpeg","image/jpg","image/gif","image/webp"]

def lambda_handler(event, context):
    # body = json.loads(event.get("body","{}"))
    raw_body = event.get("body") or "{}"
    body = json.loads(raw_body) if isinstance(raw_body, str) else raw_body
    if body.get("contentType") not in ALLOWED:
        return resp(400,"Invalid type")
    key = f"uploads/{int(time.time())}-{body['filename']}"
    url = s3.generate_presigned_url(
        "put_object",
        Params={"Bucket":BUCKET,"Key":key,"ContentType":body["contentType"]},
        ExpiresIn=60
    )
    return {"statusCode":200,"headers":{"Access-Control-Allow-Origin":"*"},"body":json.dumps({"uploadUrl":url})}

def resp(c,m):
    return {"statusCode":c,"headers":{"Access-Control-Allow-Origin":"*"},"body":json.dumps({"error":m})}