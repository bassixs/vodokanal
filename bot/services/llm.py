import aiohttp
import os
import logging
import json

logger = logging.getLogger(__name__)

class YandexGPTService:
    def __init__(self):
        self.api_key = os.getenv('YANDEX_API_KEY')
        self.iam_token = os.getenv('YANDEX_IAM_TOKEN')
        self.folder_id = os.getenv('YANDEX_FOLDER_ID')
        self.base_url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
    
    def _get_headers(self):
        headers = {
            "Content-Type": "application/json",
            "x-folder-id": self.folder_id
        }
        if self.api_key:
            headers["Authorization"] = f"Api-Key {self.api_key}"
        elif self.iam_token:
             headers["Authorization"] = f"Bearer {self.iam_token}"
        else:
             raise ValueError("No Yandex Cloud credentials provided (API Key or IAM Token)")
        return headers

    async def analyze_text(self, text: str) -> str:
        """Analyzes text using YandexGPT to provide summary and sentiment."""
        if not self.folder_id:
             logger.warning("YANDEX_FOLDER_ID not set, skipping LLM analysis.")
             return "Невозможно выполнить анализ: YANDEX_FOLDER_ID не настроен."

        headers = self._get_headers()
        
        prompt_text = (
            "Ты - профессиональный редактор и аналитик. "
            "Твоя задача обработать текст распознанного голосового сообщения.\n"
            "Выполни следующие действия:\n"
            "1. **Литературная обработка**: Перепиши исходный текст, расставь знаки препинания, исправь ошибки, разбей на абзацы для удобного чтения. Смысл должен сохраниться полностью.\n"
            "2. **Саммари**: Напиши краткую суть сообщения (1-2 предложения).\n"
            "3. **Тональность**: Определи эмоциональную окраску (Позитив, Негатив, Нейтрально, Взволнованно, Требовательно и т.д.).\n\n"
            "Формат твоего ответа должен быть строго таким:\n"
            "📖 **Исправленный текст:**\n"
            "[Здесь твой исправленный текст]\n\n"
            "📋 **Саммари:** [Текст саммари]\n"
            "🎭 **Тональность:** [Тональность]\n\n"
            "Исходный текст для обработки:\n"
            f"{text}"
        )

        body = {
            "modelUri": f"gpt://{self.folder_id}/yandexgpt/latest",
            "completionOptions": {
                "stream": False,
                "temperature": 0.4, 
                "maxTokens": "2000" # Increased to allow full text rewrite
            },
            "messages": [
                {
                    "role": "system",
                    "text": "Ты - умный ассистент-редактор."
                },
                {
                    "role": "user",
                    "text": prompt_text
                }
            ]
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.base_url, json=body, headers=headers) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"YandexGPT API error: {response.status} - {error_text}")
                        return f"Ошибка анализа (API {response.status})"
                    
                    data = await response.json()
                    # Parse YandexGPT response structure
                    # Response format: result -> alternatives -> [messages -> text]
                    alternatives = data.get("result", {}).get("alternatives", [])
                    if alternatives:
                        return alternatives[0].get("message", {}).get("text", "Нет ответа")
                    
                    return "Не удалось получить ответ от модели."

        except Exception as e:
            logger.error(f"Error calling YandexGPT: {e}", exc_info=True)
            return f"Ошибка при обращении к нейросети: {str(e)}"
