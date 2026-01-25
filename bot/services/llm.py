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
Проанализируй следующий диалог.
Текст:
{text}

НЕОБХОДИМО ВЕРНУТЬ JSON СЛЕДУЮЩЕЙ СТРУКТУРЫ:
{{
    "summary": "Краткое содержание диалога",
    "sentiment": "Тональность (Позитивно/Нейтрально/Негативно)",
    "address": "Полный адрес или 'Не указан'",
    "dialog_type": "Тип (Консультация/Жалоба/Проблема/Другое)",
    
    "is_relevant_hard": true/false, 
    // ВКЛЮЧАТЬ (true): Проблемы ХВС, слабый напор, утечка, канализация, прорыв, засор, авария на сетях.
    // ИСКЛЮЧАТЬ (false): Оплата, квитанции, личный кабинет, счетчики, справки, свет, газ, тепло, ГВС (горячая вода), открытые люки, контактные данные других отделов.

    "stats_categories": {{
        "refusal_deadline": true/false, // Оператор отказался назвать сроки завершения работ (не знает, нет информации) ИЛИ сказал 'Заявка принята/передадим' БЕЗ указания сроков.
        "no_brigade": true/false,       // Нет конкретики о направлении бригады (или "нет людей", "все заняты").
        "long_duration": true/false,    // Авария длится более суток (из контекста).
        "redirect_other_org": true/false // Оператор перенаправил в другую организацию при массовой жалобе (2+ дома).
    }},
    
    "resident_phrase": "Цитата вопроса жителя, на который получен отказ/нет ответа (если есть)",
    "accident_duration": "Длительность проблемы текстом (напр. '2 дня', 'с утра', 'неделю') или пустая строка",

    "location": {{
        "street": "Название улицы (без ул/пр)", 
        "house": "Номер дома (только цифры/буквы)"
    }},

    "markers": [
        {{ "marker_type": "Тип (Грубость/Отказ/Некомпетентность)", "operator_phrase": "Цитата" }}
    ],
    "cleaned_dialogue": "Текст диалога по ролям..."
}}

ИНСТРУКЦИИ:
1. Игнорируй вступление автоответчика.
2. ВАЖНО: is_relevant_hard ставишь true ТОЛЬКО если проблема касается ХВС (холодной воды) или канализации.
3. ЕЩЕ ВАЖНЕЕ: Если is_relevant_hard = false (тема про оплату, свет, счетчики и т.д.), то массив "markers" ДОЛЖЕН БЫТЬ ПУСТЫМ [], даже если оператор вел себя грубо или отказывал. Мы не считаем статистику по непрофильным темам.
4. Отказ в сроках (refusal_deadline = true) ставится также, если оператор отвечает общими фразами "Заявку приняли", "Передадим", "Ждите", но НЕ называет конкретных сроков устранения.
5. location: если адреса нет, оставь поля пустыми строками "". 
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
