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
            "Ты — аналитик в организации, оценивающий качество работы диспетчеров Водоканала."
            "Твоя задача — проверять диалоги на наличие ошибок и маркеров отказа."
            "Отвечай ТОЛЬКО валидным JSON объектом."
        )

        user_prompt = (
            f"""
Проанализируй диалог между Жителем и Оператором.
Текст:
{text}

<РОЛЬ>
Ты работаешь аналитиком в организации, которая занимается оценкой качества работы операторов (диспетчеров) ресурсоснабжающих организаций Калужской области в части предоставления информации жителям о ходе устранения аварийных ситуаций и плановых отключений. Ежедневно ты просматриваешь текст с распечаткой разговора (оператора) диспетчера с жителем и проверяешь диалог на наличие определенных ошибок.
</РОЛЬ>

<ЗАДАЧА>
Твоя цель — извлечь данные для сводной таблицы.
Важно: анализировать только те диалоги, из содержания которых однозначно понятно, что проблема находится в зоне ответственности Водоканала и связана с отсутствием холодной воды или утечкой, в том числе канализационной.
</ЗАДАЧА>

<КОНТЕКСТ ЗАДАЧИ (Критерии фильтрации is_relevant_hard)>
Для включения диалога (is_relevant_hard = true) должны одновременно соблюдаться условия:
1) Проблема относится к зоне ответственности Водоканала.
2) Проблема связана с отсутствием холодной воды, плохим напором воды, утечкой, в том числе канализационной, прорывом, засором, аварией на сетях.

НЕ ВКЛЮЧАТЬ (is_relevant_hard = false): вопросы по оплате, перерасчетам, квитанциям, проблемы с личным кабинетом, передачей показаний, заменой и поверкой счетчиков воды, ОТКРЫТЫМ ЛЮКАМ, запросы контактных данных других отделов, жалобы на отключения электроэнергии и газа.
ВАЖНО: Если диалог не подходит по критериям — массив markers должен быть пустым [], статистика не считается.
</КОНТЕКСТ ЗАДАЧИ>

<ОЖИДАЕМЫЙ РЕЗУЛЬТАТ (JSON Structure)>
{{
    "summary": "Краткое содержание",
    "sentiment": "Тональность (Позитивно/Нейтрально/Негативно)",
    "address": "Адрес аварии (или 'Не указан')",
    "dialog_type": "Тип реплики жителя (Консультация/Сообщение о проблеме/Жалоба)",
    
    "is_relevant_hard": true/false, 

    "stats_categories": {{
        "refusal_deadline": true/false, // 1) Отказ оператора предоставить информацию о сроках (или "приняли/передадим" без сроков), когда авария в компетенции водоканала. (НЕ включать, если даны ориентировочные сроки).
        "no_brigade": true/false,       // 2) Отсутствие факта/сроков направления бригады (или "нет свободных бригад"). (НЕ включать перенаправления в УК).
        "long_duration": true/false,    // 3) Длительные сроки аварии (более суток из контекста).
        "redirect_other_org": true/false // 4) Перенаправление в другую организацию при массовой жалобе (2+ дома на улице).
    }},
    
    "resident_phrase": "Цитата вопроса жителя, на который получен отказ/нет ответа",
    "accident_duration": "Длительность аварии текстом (напр. '2 дня', 'с утра') или пустая строка",

    "location": {{
        "street": "Улица (только название)", 
        "house": "Номер дома"
    }},

    "markers": [
        {{ "marker_type": "Тип из Справочника", "operator_phrase": "Цитата оператора" }}
    ],
    "cleaned_dialogue": "Текст диалога по ролям..."
}}
</ОЖИДАЕМЫЙ РЕЗУЛЬТАТ>

<СПРАВОЧНИК МАРКЕРОВ ОТКАЗА>
Ищи похожие фразы:
1. Прямые отказы: "Не могу", "Не смогу", "Это не к нам", "Вы позвонили не туда", "Это не наша компетенция", "Мы этим не занимаемся", "Нет свободных бригад", "Все бригады заняты".
2. Отсутствие информации: "Информации нет", "Я не знаю", "Пока не могу сказать", "Сложно сказать", "Неизвестно", "Данные не уточняются".
3. Отсрочка: "Ждите", "Потерпите", "Позвоните позже", "Перезвоните завтра", "Сейчас посмотрю" (без сроков), "Ждём информацию", "приняли/передадим заявку" (без сроков).
4. Отсутствие сроков: "Вскоре", "В ближайшее время", "Как только возможно", "Когда получим информацию", "Как освободится бригада", "Ориентируйтесь на..." (без точности).
5. Перекладывание: "Сообщим мастеру", "Уже передали...", "Это к кому-то другому", "Обратитесь в..." (без конкретики), "Мы уже сообщали...".
6. Минимизация: "Это нормально", "Ничего страшного", "Вообще не проблема", "Все так живут".
7. Невнимание: "Угу", "Ладно", "Хорошо" (без деталей), Молчание.
8. Невозможность контакта: "Номер не будет записан", "Обратной связи не будет", "Вам не перезвонят".
</СПРАВОЧНИК МАРКЕРОВ ОТКАЗА>

ИНСТРУКЦИИ:
1. Игнорируй вступление автоответчика.
2. dialog_type только: "Консультация", "Сообщение о проблеме", "Жалоба".
3. STRICT MODE: Если is_relevant_hard = false, то markers = [].
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
