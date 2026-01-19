# Yandex SpeechKit Telegram Bot

Асинхронное распознавание голосовых сообщений с использованием Yandex Cloud.

## Структура
- `bot/main.py`: Точка входа.
- `bot/handlers.py`: Логика бота.
- `bot/services/`: Интеграция с Yandex Cloud.

## Установка

```bash
pip install -r requirements.txt
cp .env.example .env
# Отредактируйте .env
python -m bot.main
```
## Переменные окружения (.env)
- `TELEGRAM_BOT_TOKEN`: Токен от @BotFather
- `YANDEX_API_KEY`: API ключ сервисного аккаунта Yandex
- `AWS_ACCESS_KEY_ID`: ID ключа Static Access Key
- `AWS_SECRET_ACCESS_KEY`: Секрет Static Access Key
- `BUCKET_NAME`: Имя бакета
