from fastapi import FastAPI, File, UploadFile, HTTPException
import boto3
import os
import uuid
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv(override=True)

app = FastAPI()

# AWS Configurations
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
SQS_QUEUE_NAME = os.getenv("SQS_QUEUE_NAME")
DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")  

# AWS Clients
s3_client = boto3.client("s3", region_name=AWS_REGION)
sqs_client = boto3.resource("sqs", region_name=AWS_REGION)
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)

table = dynamodb.Table(DYNAMODB_TABLE_NAME)
queue = sqs_client.get_queue_by_name(QueueName=SQS_QUEUE_NAME)  
print(f"Using SQS Queue: {SQS_QUEUE_NAME, queue}")

@app.post("/upload/")
async def upload_image(file: UploadFile = File(...)):
   
    # Generate unique file name
    file_extension = file.filename.split(".")[-1]
    unique_id = str(uuid.uuid4())
    s3_key = f"uploads/{unique_id}.{file_extension}"

    # Upload to S3
    s3_client.upload_fileobj(file.file, S3_BUCKET_NAME, s3_key, ExtraArgs={"ContentType": file.content_type})
    image_url = f"https://{S3_BUCKET_NAME}.s3.amazonaws.com/{s3_key}"

    # Create job record in DynamoDB
    enhancement_id = unique_id
    created_at = round(time.time())

    table.put_item(
        Item={
            "enhancementId": enhancement_id,
            "imageUrl": image_url,
            "createdAt": created_at,
            "status": "pending"
        }
    )

    message_deduplication_id = str(uuid.uuid4())

    response = queue.send_message(MessageBody=str({"enhancementId": enhancement_id, "imageUrl": image_url}),
        MessageGroupId="default-group",  # FIFO queues require a MessageGroupId
        MessageDeduplicationId=message_deduplication_id  # Ensuring uniqueness
        )


    return {"message": "File uploaded and job created", "enhancementId": enhancement_id}
    

@app.get("/status/{enhancement_id}")
async def check_status(enhancement_id: str):
    try:
        response = table.get_item(Key={"enhancementId": enhancement_id})
        if "Item" not in response:
            raise HTTPException(status_code=404, detail="Job not found")

        return response["Item"]
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving status: {str(e)}")

@app.get("/result/{enhancement_id}")
async def get_result(enhancement_id: str):
    try:
        response = table.get_item(Key={"enhancementId": enhancement_id})
        if "Item" not in response:
            raise HTTPException(status_code=404, detail="Job not found")

        job = response["Item"]
        if job.get("status") != "completed":
            return {"message": "Job is still in progress", "status": job.get("status")}

        enhanced_image_url = f"https://{S3_BUCKET_NAME}.s3.amazonaws.com/{job['enhancedImageS3Key']}"
        return {"message": "Enhancement completed", "image_url": enhanced_image_url}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving result: {str(e)}")
