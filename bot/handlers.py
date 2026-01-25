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
        # REQUIRED 7 COLUMNS:
        # 1 столбец – номер диалога
        # 2 столбец – номер аудиофайла (имя файла)
        # 3 столбец – текст звонка (result_text)
        # 4 столбец – фраза жителя (resident_phrase)
        # 5 столбец – фраза оператора (из маркеров или пустая? User says "фраза оператора". We only store 'refusal_marker' which contains phrases and type)
        # 6 столбец – маркер отказа (Same as above? Or split? User list in 5 and 6 columns suggests separate. But we have 'refusal_marker' stored as combined string.)
        # Let's map 'refusal_marker' to both for now or just put it in one and leave other empty if we can't split easily.
        # Actually, let's just map as requested:
        
        column_map = {
            'id': 'Номер диалога',
            'file_name': 'Номер аудиофайла', 
            'result_text': 'Текст звонка',
            'resident_phrase': 'Фраза жителя',
            'refusal_marker': 'Маркер отказа', # This contains "Type (Phrase)"
            'accident_duration': 'Длительность аварии',
        }
        
        # We need a 5th column "Фраза оператора". Since our `refusal_marker` field is "Type ('Phrase')", 
        # we can try to duplicate it or just provide the full marker string in both if fuzzy.
        # Ideally we would split it, but for now let's reuse.
        # Wait, column 5 is "Phrase Operator", column 6 is "Marker".
        # Let's add a calculated column for "Operator Phrase" based on "Refusal Marker" string.
        
        if 'refusal_marker' in df.columns:
            # Simple extraction regex if format is "Type ('Phrase')"
            # If multiple markers, it's semicolon separated.
            # Let's just copy the column for now to ensure column exists.
            df['Фраза оператора'] = df['refusal_marker'] 
        else:
             df['Фраза оператора'] = ""

        ordered_columns = [
            'Номер диалога', 
            'Номер аудиофайла', 
            'Текст звонка', 
            'Фраза жителя', 
            'Фраза оператора', 
            'Маркер отказа', 
            'Длительность аварии'
        ]
        
        # Filter only existing columns (using mapped names)
        # First rename what we can
        rename_map = {k:v for k,v in column_map.items() if k in df.columns}
        export_df = df.rename(columns=rename_map)
        
        # Ensure all ordered columns exist (add empty if missing)
        for col in ordered_columns:
            if col not in export_df.columns:
                export_df[col] = ""
                
        # Select final order
        export_df = export_df[ordered_columns]
        
        filename = f"export_{message.from_user.id}.xlsx"
        export_df.to_excel(filename, index=False)
        
        input_file = FSInputFile(filename)
        await message.reply_document(input_file, caption="📊 Выгрузка таблицы (7 столбцов)")
        
        os.remove(filename)
        await msg.delete()
        
    except Exception as e:
        logger.error(f"Export error: {e}", exc_info=True)
        await msg.edit_text(f"❌ Ошибка выгрузки: {e}")

@router.message(Command("stats"))
async def command_stats_handler(message: Message):
    """
    Generates a statistical report based on V3.1 requirements.
    1. Counts for 4 specific categories.
    2. Street clustering (streets with complaints from >= 2 distinct houses).
    """
    status_msg = await message.reply("📊 Считаю статистику...")
    
    try:
        tasks = await db_service.get_all_tasks()
        
        # 1. Category Counters
        cat_refusal = 0
        cat_no_brigade = 0
        cat_long = 0
        cat_redirect = 0
        
        # 2. Street Clustering Data
        # Structure: { "street_name": { "house_1", "house_2" } }
        street_map = {}
        
        relevant_count = 0
        
        for t in tasks:
            # Only consider relevant tasks that passed the hard filter
            if t.get('is_relevant_hard'):
                relevant_count += 1
                
                # Categories
                if t.get('category_refusal_works'): cat_refusal += 1
                if t.get('category_no_brigade'): cat_no_brigade += 1
                if t.get('category_long_duration'): cat_long += 1
                if t.get('category_redirect'): cat_redirect += 1
                
                # Clustering
                street = t.get('cleaned_street')
                house = t.get('cleaned_house')
                
                if street and house:
                    # Normalize strict comparison
                    s_norm = street.strip().lower()
                    h_norm = house.strip().lower()
                    
                    if s_norm not in street_map:
                        street_map[s_norm] = {'name': street, 'houses': set()}
                    
                    street_map[s_norm]['houses'].add(h_norm)

        # Build Report
        report = (
            f"📈 **Аналитическая Сводка (V3.1)**\n"
            f"Всего релевантных диалогов: {relevant_count}\n\n"
            f"🔍 **По категориям проблем:**\n"
            f"1. 🚫 Отказ в сроках: **{cat_refusal}**\n"
            f"2. 🚒 Нет бригады: **{cat_no_brigade}**\n"
            f"3. ⏳ Длительная (>24ч): **{cat_long}**\n"
            f"4. ↪️ Перенаправление: **{cat_redirect}**\n\n"
            f"🏘 **Проблемные улицы (2+ дома):**\n"
        )
        
        # Filter streets with >= 2 distinct houses
        problem_streets = []
        for s_key, data in street_map.items():
            if len(data['houses']) >= 2:
                # Format: "ул. Ленина (д. 5, 7)"
                houses_str = ", ".join(sorted(list(data['houses'])))
                problem_streets.append(f"- {data['name']} (д. {houses_str}) — {len(data['houses'])} заяв(ок)")
        
        if problem_streets:
            report += "\n".join(problem_streets)
        else:
            report += "✅ Массовых аварий (разные дома на одной улице) не выявлено."
            
        await status_msg.edit_text(report, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Stats error: {e}", exc_info=True)
        await status_msg.edit_text(f"❌ Ошибка статистики: {e}")

@router.message(Command("clean"))
async def command_clean_handler(message: Message):
    """
    Cleans up storage manually (S3 and Local).
    """
    logger.info(f"Received /clean command from user {message.from_user.id}")
    status_msg = await message.reply("🧹 Начинаю очистку хранилища...")
    
    try:
        # 1. Clean S3
        # Clean S3 (FULL WIPE)
        # We assume the storage service instance from global var or create new
        storage = YandexStorageService()
        
        # Clean Everything
        count_s3 = await storage.cleanup_all()
        
        # 2. Clean Local
        import glob
        local_files = glob.glob("temp_*") + glob.glob("transcript_*") + glob.glob("export_*")
        count_local = 0
        for f in local_files:
            try:
                os.remove(f)
                count_local += 1
            except:
                pass
                
        report = (
            f"✅ **Очистка завершена!**\n\n"
            f"☁️ **Яндекс S3:**\n"
            f"- Удалено объектов: {count_s3}\n\n"
            f"🖥 **Локальный диск:**\n"
            f"- Удалено файлов: {count_local}"
        )
        await status_msg.edit_text(report, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Cleanup error: {e}", exc_info=True)
        await status_msg.edit_text(f"❌ Ошибка очистки: {e}")

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
        mime = str(message.document.mime_type).lower()
        fname = message.document.file_name.lower() if message.document.file_name else ""
        
        is_zip = mime == 'application/zip' or fname.endswith('.zip')
        is_rar = 'rar' in mime or fname.endswith('.rar')
        
        if is_zip or is_rar:
             # Archive
             pass # Accepted
        elif not mime.startswith('audio/'):
            await message.reply("📂 Это не аудиофайл и не архив. Пожалуйста, отправьте аудио или .zip/.rar архив.")
            return
            
        file_id = message.document.file_id
        original_name = message.document.file_name or "document"
        
        # Determine effective file type
        effective_content_type = message.content_type
        if is_zip:
            effective_content_type = 'application/zip'
        elif is_rar:
            effective_content_type = 'application/x-rar-compressed'
    else:
        return

    # Add to Queue
    try:
        task_id = await db_service.add_task(
            user_id=user_id, 
            file_type=effective_content_type, 
            source_path=file_id, 
            file_name=original_name
        )
        
        await message.reply(f"📥 **Принято в обработку!**\nНомер задачи: `#{task_id}`\n\nЯ уведомлю вас, когда результат будет готов.")
        
    except Exception as e:
        logger.error(f"Failed to queue task: {e}", exc_info=True)
        await message.reply("❌ Ошибка при добавлении в очередь.")
