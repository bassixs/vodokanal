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
        
        # Determine encoding from extension
        encoding = "OGG_OPUS" # Default usually expected by Yandex if not specified? 
        # Actually docs say: "For LPCM ... required. For others ... optional".
        # But "ogg header not found" usually means it sees bits but expects OGG.
        
        lower_url = file_url.lower()
        if lower_url.endswith(".mp3"):
            encoding = "MP3"
        elif lower_url.endswith(".ogg"):
             encoding = "OGG_OPUS"
        else:
             encoding = None # Let it auto-detect or default
             
        specification = {
            "languageCode": "ru-RU",
            "model": "general:rc", # Release Candidate - engine from V3
            "literature_text": True, # Punctuation and normalization
            "profanity_filter": False, # Keep everything for analysis
        }
        
        if encoding:
            specification["audioEncoding"] = encoding
        
        body = {
            "config": {
                "specification": specification
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
                    
                    # Debug logging
                    logger.info(f"SpeechKit returned {len(chunks)} chunks")
                    if chunks:
                        logger.info(f"First chunk sample: {chunks[0]}")
                    
                    # Simple text concatenation - NO speaker splitting (doesn't work for mono audio)
                    # Speaker diarization will be done by YandexGPT in cleaned_dialogue
                    text_parts = []
                    for chunk in chunks:
                        alt = chunk.get("alternatives", [{}])[0]
                        text = alt.get("text", "")
                        if text:
                            text_parts.append(text)

                    full_text = " ".join(text_parts)
                    logger.info(f"Concatenated {len(text_parts)} text segments")
                    return full_text
                
                return None

    async def wait_for_completion(self, operation_id: str, poll_interval: int = 2) -> str:
        """Waits for the operation to complete properly."""
        while True:
            result = await self.get_result(operation_id)
            if result is not None:
                return result
            await asyncio.sleep(poll_interval)
