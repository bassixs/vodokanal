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
        """Analyzes text using YandexGPT with the specific Vodokanal Quality Control prompt."""
        if not self.folder_id:
             logger.warning("YANDEX_FOLDER_ID not set, skipping LLM analysis.")
             return "Невозможно выполнить анализ: YANDEX_FOLDER_ID не настроен."

        headers = self._get_headers()
        
        # User defined prompt (shortened slightly for efficiency but keeping all logic)
        # User defined prompt
        system_prompt = (
            "Ты — опытный аналитик Водоканала. Твоя задача — извлечь информацию из диалога."
            "Отвечай ТОЛЬКО валидным JSON объектом. Не используй Markdown блоки (```json)."
        )

        user_prompt = (
            f"""
Проанализируй следующий диалог между Оператором и Жителем.
Текст диалога:
{text}

НЕОБХОДИМО ВЕРНУТЬ JSON СЛЕДУЮЩЕЙ СТРУКТУРЫ:
{{
    "summary": "Краткое содержание диалога (1 предложение)",
    "sentiment": "Тональность: Позитивно, Нейтрально или Негативно",
    "address": "Адрес проблемы (если есть, иначе 'Не указан')",
    "dialog_type": "Тип: Консультация, Жалоба, Проблема или Другое",
    "is_relevant": true/false (Относится ли к водоснабжению/канализации),
    "markers": [
        {{ "marker_type": "Тип нарушения (Грубость/Отказ/Некомпетентность)", "operator_phrase": "Цитата оператора" }}
    ],
    "cleaned_dialogue": "Текст диалога, разделенный по ролям: '- Житель: ... \\n - Оператор: ...'"
}}

ИНСТРУКЦИИ:
1. Игнорируй вступление автоответчика.
2. 'markers' — это список нарушений оператора (грубость, отказ помочь, отсутствие сроков). Если нарушений нет, верни пустой список [].
3. 'cleaned_dialogue' должен быть одной строкой с переносами \\n.

ПРИМЕР ОТВЕТА:
{{
    "summary": "Житель сообщил об утечке на Ленина 1, оператор принял заявку.",
    "sentiment": "Нейтрально",
    "address": "ул. Ленина, д. 1",
    "dialog_type": "Проблема",
    "is_relevant": true,
    "markers": [],
    "cleaned_dialogue": "- Житель: У нас вода течет на улице.\\n- Оператор: Принято, выезжаем."
}}
            """
        )

        body = {
            "modelUri": f"gpt://{self.folder_id}/yandexgpt/latest",
            "completionOptions": {
                "stream": False,
                "temperature": 0.3, 
                "maxTokens": 2000 
            },
            "messages": [
                {
                    "role": "system",
                    "text": system_prompt
                },
                {
                    "role": "user",
                    "text": user_prompt
                }
            ]
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.base_url, json=body, headers=headers) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"YandexGPT API error: {response.status} - {error_text}")
                        # Return error as JSON so worker can parse it
                        return json.dumps({
                            "summary": f"Ошибка AI: {response.status}",
                            "sentiment": "ERROR",
                            "address": "Ошибка",
                            "dialog_type": "Ошибка",
                            "cleaned_dialogue": f"Ошибка API: {error_text}"
                        }, ensure_ascii=False)
                    
                    data = await response.json()
                    alternatives = data.get("result", {}).get("alternatives", [])
                    if alternatives:
                        result_text = alternatives[0].get("message", {}).get("text", "{}")
                        logger.info(f"LLM Raw Response: {result_text[:200]}...") # Log first 200 chars
                        return result_text
                    
                    # Log the weird response
                    logger.warning(f"YandexGPT returned no alternatives. Full response: {data}")
                    return json.dumps({
                        "summary": "Ошибка AI: Пустой ответ", 
                        "cleaned_dialogue": f"Raw response: {data}"
                    }, ensure_ascii=False)

        except Exception as e:
            logger.error(f"Error calling YandexGPT: {e}", exc_info=True)
            return json.dumps({
                "summary": f"Ошибка клиента: {e}", 
                "cleaned_dialogue": f"Exception: {e}"
            }, ensure_ascii=False)
