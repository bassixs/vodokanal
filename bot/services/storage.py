import boto3
import os
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class YandexStorageService:
    def __init__(self):
        self.session = boto3.session.Session()
        self.s3 = self.session.client(
            service_name='s3',
            endpoint_url='https://storage.yandexcloud.net',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )
        self.bucket_name = os.getenv('BUCKET_NAME')

    async def upload_file(self, file_path: str, object_name: str) -> str:
        """Upload a file to an S3 bucket and return the URL."""
        if object_name is None:
            object_name = os.path.basename(file_path)

        try:
            # Upload the file
            # Note: boto3 is synchronous. In a high-load async app, we should run this in an executor.
            # For this task, direct call is acceptable or wrapping in run_in_executor.
            # We will use direct call for simplicity as boto3 operations are blocking but fast for small files.
            # Better approach for async aiogram is run_in_executor.
            import asyncio
            from functools import partial
            
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None, 
                partial(self.s3.upload_file, file_path, self.bucket_name, object_name)
            )
            
            url = f"https://storage.yandexcloud.net/{self.bucket_name}/{object_name}"
            logger.info(f"File uploaded to {url}")
            return url
        except ClientError as e:
            logger.error(f"Failed to upload file to S3: {e}")
            raise
