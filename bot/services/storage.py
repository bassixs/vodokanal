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

    async def delete_file(self, object_name: str):
        """Deletes a file from S3."""
        import asyncio
        from functools import partial
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                partial(self.s3.delete_object, Bucket=self.bucket_name, Key=object_name)
            )
            logger.info(f"Deleted file from S3: {object_name}")
        except ClientError as e:
            logger.error(f"Failed to delete file {object_name}: {e}")

    async def cleanup_prefix(self, prefix: str):
        """Deletes all files starting with prefix."""
        import asyncio
        from functools import partial
        try:
            loop = asyncio.get_running_loop()
            
            # List objects
            response = await loop.run_in_executor(
                None,
                partial(self.s3.list_objects_v2, Bucket=self.bucket_name, Prefix=prefix)
            )
            
            if 'Contents' in response:
                objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
                if objects_to_delete:
                    await loop.run_in_executor(
                        None,
                        partial(self.s3.delete_objects, Bucket=self.bucket_name, Delete={'Objects': objects_to_delete})
                    )
                    logger.info(f"Deleted {len(objects_to_delete)} files with prefix '{prefix}'")
                    return len(objects_to_delete)
            return 0
        except ClientError as e:
            logger.error(f"Failed to cleanup prefix {prefix}: {e}")
            return 0
