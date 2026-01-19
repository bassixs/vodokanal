import os
import logging
from aiogram import Router, F, Bot
from aiogram.types import Message, ContentType
from aiogram.filters import CommandStart
from bot.services.storage import YandexStorageService
from bot.services.speechkit import SpeechKitService

from bot.services.llm import YandexGPTService

router = Router()
logger = logging.getLogger(__name__)

# Initialize services
storage_service = YandexStorageService()
speechkit_service = SpeechKitService()
llm_service = YandexGPTService()

@router.message(CommandStart())
async def command_start_handler(message: Message):
    await message.answer("Привет! Отправь мне голосовое сообщение, и я распознаю его через Yandex SpeechKit (+ Анализ YandexGPT).")

@router.message(F.content_type.in_([ContentType.VOICE, ContentType.AUDIO, ContentType.DOCUMENT]))
async def voice_message_handler(message: Message, bot: Bot):
    user_id = message.from_user.id
    
    # Determine file_id and file_name based on content type
    if message.content_type == ContentType.VOICE:
        file_id = message.voice.file_id
        file_unique_id = message.voice.file_unique_id
        original_name = "voice.ogg" # Voices don't have filenames usually
    elif message.content_type == ContentType.AUDIO:
        file_id = message.audio.file_id
        file_unique_id = message.audio.file_unique_id
        original_name = message.audio.file_name or "audio.mp3"
    elif message.content_type == ContentType.DOCUMENT:
        if not message.document.mime_type.startswith('audio/'):
            # Ignore non-audio documents or reply with error
            # For now, let's just ignore or reply nicely
            await message.reply("📂 Я вижу файл, но это не похоже на аудио. Пожалуйста, отправьте аудиофайл.")
            return
        file_id = message.document.file_id
        file_unique_id = message.document.file_unique_id
        original_name = message.document.file_name or "document.audio"
    else:
        return

    status_msg = await message.reply("⏳ Скачиваю аудиофайл...")
    
    try:
        # 1. Download file from Telegram
        file = await bot.get_file(file_id)
        file_path = file.file_path
        
        # Local temporary path (using unique id to avoid collisions)
        temp_filename = f"{user_id}_{file_unique_id}_{original_name}"
        await bot.download_file(file_path, temp_filename)
        
        # 2. Upload to Yandex Object Storage
        await status_msg.edit_text("⏳ Загружаю в Yandex Object Storage...")
        object_name = f"voice/{temp_filename}"
        file_url = await storage_service.upload_file(temp_filename, object_name)
        
        # Cleanup local file
        if os.path.exists(temp_filename):
            os.remove(temp_filename)
            
        # 3. Submit to SpeechKit
        await status_msg.edit_text("⏳ Отправляю на распознавание...")
        operation_id = await speechkit_service.submit_recognition(file_url)
        
        # 4. Wait for result
        await status_msg.edit_text("⏳ Ожидаю результат распознавания...")
        text = await speechkit_service.wait_for_completion(operation_id)
        
        if text:
            await status_msg.edit_text("🧠 Обрабатываю текст и анализирую (YandexGPT)...")
            analysis = await llm_service.analyze_text(text)
            
            await status_msg.delete()
            
            # Check length limit (Telegram limit is 4096, keep safety margin)
            if len(analysis) > 4000:
                # Create a text file
                result_filename = f"analysis_{user_id}_{file_unique_id}.txt"
                with open(result_filename, "w", encoding="utf-8") as f:
                    f.write(analysis)
                
                from aiogram.types import FSInputFile
                input_file = FSInputFile(result_filename)
                
                await message.reply_document(
                    input_file, 
                    caption="📄 Текст слишком длинный, отправляю файлом."
                )
                
                # Try to extract Summary and Sentiment for the caption/message
                try:
                    # Simple extraction based on our Prompt format
                    summary_start = analysis.find("📋 **Саммари:**")
                    if summary_start != -1:
                        short_info = analysis[summary_start:]
                        if len(short_info) < 1000: # Ensure caption limit (1024 chars)
                             await message.reply(short_info, parse_mode="Markdown")
                except Exception:
                    pass

                # Cleanup result file
                if os.path.exists(result_filename):
                    os.remove(result_filename)
            else:
                await message.reply(analysis, parse_mode="Markdown")
        else:
            await status_msg.edit_text("❌ Не удалось распознать текст или аудио пустое.")

    except TimeoutError:
        logger.error("Timeout downloading file")
        await status_msg.edit_text("❌ Ошибка: Файл слишком долго скачивается. Попробуйте файл поменьше.")
        if 'temp_filename' in locals() and os.path.exists(temp_filename):
            os.remove(temp_filename)
    except Exception as e:
        logger.error(f"Error handling audio message: {e}", exc_info=True)
        await status_msg.edit_text(f"❌ Произошла ошибка: {str(e)}")
        if 'temp_filename' in locals() and os.path.exists(temp_filename):
            os.remove(temp_filename)
