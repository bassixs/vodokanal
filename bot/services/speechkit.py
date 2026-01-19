import aiohttp
import os
import logging
import asyncio
from typing import Optional

logger = logging.getLogger(__name__)

class SpeechKitService:
    def __init__(self):
        self.api_key = os.getenv('YANDEX_API_KEY')
        self.iam_token = os.getenv('YANDEX_IAM_TOKEN') # Optional, if using IAM
        self.base_url = "https://transcribe.api.cloud.yandex.net/speech/stt/v2/longRunningRecognize"
    
    def _get_headers(self):
        if self.api_key:
            return {"Authorization": f"Api-Key {self.api_key}"}
        elif self.iam_token:
             return {"Authorization": f"Bearer {self.iam_token}"}
        else:
             raise ValueError("No Yandex Cloud credentials provided (API Key or IAM Token)")

    async def submit_recognition(self, file_url: str) -> str:
        """Submits an audio file for asynchronous recognition."""
        headers = self._get_headers()
        body = {
            "config": {
                "specification": {
                    "languageCode": "ru-RU",
                    "model": "general", # or deferred-general
                    # "audioEncoding": "MP3", # Optional, usually auto-detected
                }
            },
            "audio": {
                "uri": file_url
            }
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(self.base_url, json=body, headers=headers) as response:
                if response.status != 200:
                    text = await response.text()
                    logger.error(f"SpeechKit API error: {response.status} - {text}")
                    raise Exception(f"SpeechKit API error: {response.status}")
                
                data = await response.json()
                operation_id = data.get("id")
                logger.info(f"Recognition operation started: {operation_id}")
                return operation_id

    async def get_result(self, operation_id: str) -> Optional[str]:
        """Checks the status of the operation and returns the result if ready."""
        headers = self._get_headers()
        url = f"https://operation.api.cloud.yandex.net/operations/{operation_id}"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status != 200:
                    text = await response.text()
                    logger.error(f"Operation status check error: {response.status} - {text}")
                    raise Exception(f"Operation status check error: {response.status}")
                
                data = await response.json()
                
                if data.get("done"):
                    response_data = data.get("response", {})
                    chunks = response_data.get("chunks", [])
                    full_text = " ".join([chunk.get("alternatives", [{}])[0].get("text", "") for chunk in chunks])
                    return full_text
                
                return None

    async def wait_for_completion(self, operation_id: str, poll_interval: int = 2) -> str:
        """Waits for the operation to complete properly."""
        while True:
            result = await self.get_result(operation_id)
            if result is not None:
                return result
            await asyncio.sleep(poll_interval)
