import logging
from aiogram import Router, F, Bot
from aiogram.types import Message, ContentType, CallbackQuery, FSInputFile
from aiogram.filters import CommandStart, Command
import os
from bot.services.storage import YandexStorageService
from bot.services.speechkit import SpeechKitService

from bot.services.llm import YandexGPTService

router = Router()
logger = logging.getLogger(__name__)

from bot.services.database import DatabaseService

# Initialize services (Global for router)
db_service = DatabaseService()

@router.message(CommandStart())
async def command_start_handler(message: Message):
    await message.answer("Привет! Отправь мне голосовое сообщение, и я распознаю его через Yandex SpeechKit (+ Анализ YandexGPT).")

@router.message(Command("id"))
async def command_id_handler(message: Message):
    await message.reply(f"🆔 ID этого чата: `{message.chat.id}`\n(Скопируйте это в .env как TARGET_CHAT_ID)")

@router.callback_query(F.data.startswith("take_task_"))
async def callback_take_task(callback: CallbackQuery):
    task_id = callback.data.split("_")[-1]
    username = callback.from_user.username or callback.from_user.first_name
    
    # Get current text to append status
    current_text = callback.message.text
    # Or caption if it's a document/file
    if not current_text:
        current_text = callback.message.caption or ""
        
    new_text = f"{current_text}\n\n🔨 **Взял в работу:** @{username}"
    
    try:
        if callback.message.content_type == ContentType.TEXT:
            await callback.message.edit_text(new_text, reply_markup=None, parse_mode="Markdown")
        elif callback.message.content_type == ContentType.DOCUMENT:
             await callback.message.edit_caption(caption=new_text, reply_markup=None, parse_mode="Markdown")
             
        await callback.answer("Вы взяли задачу!")
    except Exception as e:
        logger.error(f"Error editing message: {e}")
        await callback.answer("Ошибка обновления статуса", show_alert=True)

@router.message(Command("export"))
async def command_export_handler(message: Message):
    import pandas as pd
    
    msg = await message.reply("⏳ Генерирую отчет...")
    
    try:
        tasks = await db_service.get_all_tasks()
        
        if not tasks:
            await msg.edit_text("📂 База данных пуста.")
            return

        # Prepare Data for Excel
        df = pd.DataFrame(tasks)
        
        if df.empty:
             await msg.edit_text("📂 База данных пуста.")
             return

        # Rename columns if they exist
        # We process whatever columns we have, failing gracefully if schema changed
        # Define nice names
        column_map = {
            'id': '№ Диалога',
            'created_at': 'Дата',
            'file_name': 'Имя файла',
            'address': 'Адрес',
            'result_text': 'Текст Диалога',  # Or Resident Phrase / Operator Phrase if we split them?
            # Ideally "Phrase Resident" and "Phrase Operator" come from the JSON markers list.
            # But in the flat table "refusal_marker" is just a string summary.
            # The User asked for specific columns: "1=No, 2=Address, 3=Resident, 4=Operator, 5=Marker"
            # Since a dialog can have MULTIPLE markers, this suggests a master-detail or just flattening.
            # For this MVP export, let's export the "refusal_marker" string which contains "Type (Phrase)".
            
            'refusal_marker': 'Маркер Отказа (Фраза оператора)',
            'dialog_type': 'Тип Обращения',
            'result_summary': 'Саммари'
        }
        
        # Filter only existing columns
        cols_to_use = [c for c in column_map.keys() if c in df.columns]
        export_df = df[cols_to_use].rename(columns=column_map)
        
        filename = f"export_{message.from_user.id}.xlsx"
        export_df.to_excel(filename, index=False)
        
        input_file = FSInputFile(filename)
        await message.reply_document(input_file, caption="📊 Выгрузка всех задач")
        
        os.remove(filename)
        await msg.delete()
        
    except Exception as e:
        logger.error(f"Export error: {e}", exc_info=True)
        await msg.edit_text(f"❌ Ошибка выгрузки: {e}")

@router.message(F.content_type.in_([ContentType.VOICE, ContentType.AUDIO, ContentType.DOCUMENT]))
async def voice_message_handler(message: Message, bot: Bot):
    user_id = message.from_user.id
    
    # Determine file_id and file_name based on content type
    if message.content_type == ContentType.VOICE:
        file_id = message.voice.file_id
        original_name = "voice.ogg" 
    elif message.content_type == ContentType.AUDIO:
        file_id = message.audio.file_id
        original_name = message.audio.file_name or "audio.mp3"
    elif message.content_type == ContentType.DOCUMENT:
        mime = message.document.mime_type
        if mime == 'application/zip' or message.document.file_name.lower().endswith('.zip'):
             # Archive
             pass # Accepted
        elif not mime.startswith('audio/'):
            await message.reply("📂 Это не аудиофайл и не архив. Пожалуйста, отправьте аудио или .zip архив.")
            return
            
        file_id = message.document.file_id
        original_name = message.document.file_name or "document"
        
        # Override file_type for archive so worker knows
        if original_name.lower().endswith('.zip'):
            message.content_type = 'application/zip'
    else:
        return

    # Add to Queue
    try:
        task_id = await db_service.add_task(
            user_id=user_id, 
            file_type=message.content_type, 
            source_path=file_id, 
            file_name=original_name
        )
        
        await message.reply(f"📥 **Принято в обработку!**\nНомер задачи: `#{task_id}`\n\nЯ уведомлю вас, когда результат будет готов.")
        
    except Exception as e:
        logger.error(f"Failed to queue task: {e}", exc_info=True)
        await message.reply("❌ Ошибка при добавлении в очередь.")
