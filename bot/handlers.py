import logging
from aiogram import Router, F, Bot
from aiogram.types import Message, ContentType, CallbackQuery, FSInputFile
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
import os
from bot.services.storage import YandexStorageService
from bot.services.speechkit import SpeechKitService
from bot.services.llm import YandexGPTService
from bot.services.database import DatabaseService
from bot.states import DateInputStates
from bot.keyboards import get_period_selection_keyboard
from bot.date_utils import parse_date, parse_period, get_preset_period, format_date_range


router = Router()
logger = logging.getLogger(__name__)

# Initialize services (Global for router)
db_service = DatabaseService()

@router.message(CommandStart())
async def command_start_handler(message: Message):
    await message.answer("Привет! Отправь мне голосовое сообщение, и я распознаю его через Yandex SpeechKit (+ Анализ YandexGPT).")

@router.message(Command("id"))
async def command_id_handler(message: Message):
    await message.reply(f"🆔 ID этого чата: `{message.chat.id}`\n(Скопируйте это в .env как TARGET_CHAT_ID)")

@router.message(Command("info"))
async def command_info_handler(message: Message):
    """Displays comprehensive user guide for the bot."""
    info_text = """
📘 **Руководство Пользователя Бота Водоканал**

**🎯 Основные Возможности:**

**1️⃣ Обработка Голосовых Сообщений**
Отправьте голосовое сообщение или аудиофайл → Бот автоматически:
• Распознает текст (Yandex SpeechKit)
• Анализирует содержание (YandexGPT)
• Определяет адрес и тип проблемы
• Создаёт задачу с категориями

**2️⃣ Команды Отчётов**

📊 `/export` — Выгрузка данных в Excel
• Выберите период из готовых вариантов:
  - За сегодня
  - За вчера
  - За последние 7 дней
  - За последние 30 дней
  - Произвольный период
  
• Для произвольного периода введите:
  - Конкретный день: `27.01.2026`
  - Диапазон: `с 20.01.2026 по 27.01.2026`

🎉 **Бонус:** После экспорта автоматически получите статистику!

📈 `/stats` — Аналитический отчёт
Получите статистику с анализом:
• Количество релевантных обращений
• Распределение по категориям проблем
• Проблемные улицы (массовые аварии)
• Работает с теми же периодами, что и `/export`

**3️⃣ Управление Данными**

🗑 `/clean` — Очистка хранилища
Удаляет все аудиофайлы из:
• Яндекс S3 (облачное хранилище)
• Локального диска сервера

**4️⃣ Служебные Команды**

🆔 `/id` — Узнать ID чата
📘 `/info` — Это руководство
🏠 `/start` — Приветствие

---

**💡 Примеры Использования**

**Сценарий 1: Быстрый Экспорт**
```
Вы: /export
Бот: [Клавиатура с периодами]
Вы: [Нажимаете "За последние 7 дней"]
Бот: 📊 Выгрузка за период... (Excel)
     📈 Аналитическая Сводка... (Статистика)
```

**Сценарий 2: Отчёт за Месяц**
```
Вы: /stats
Бот: [Клавиатура]
Вы: [Нажимаете "За последние 30 дней"]
Бот: 📈 Статистика за 30 дней
```

**Сценарий 3: Произвольный Период**
```
Вы: /export
Бот: [Клавиатура]
Вы: [Нажимаете "✏️ Произвольный период"]
Бот: Введите период...
Вы: с 01.01.2026 по 15.01.2026
Бот: 📊 Выгрузка + 📈 Статистика
```

---

**📋 Форматы Данных**

**Excel Содержит:**
1. Номер диалога
2. Номер аудиофайла
3. Текст звонка
4. Фраза жителя
5. Фраза оператора
6. Маркер отказа
7. Длительность аварии

**Статистика Включает:**
• Общее количество релевантных обращений
• Категории проблем:
  - Отказ в сроках работ
  - Отсутствие бригады
  - Длительная авария (>24ч)
  - Перенаправление звонка
• Кластеры проблемных улиц

---

**🔐 Доступ**
Все команды доступны авторизованным пользователям бота.

**❓ Вопросы?**
Обратитесь к администратору системы.

---
_Версия: 2.0 | Дата: Январь 2026_
"""
    await message.reply(info_text, parse_mode="Markdown")


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
    """
    Shows period selection keyboard for Excel export.
    """
    logger.info(f"User {message.from_user.id} requested /export")
    keyboard = get_period_selection_keyboard("export")
    await message.reply(
        "📅 **Выберите период для выгрузки:**",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )

@router.message(Command("stats"))
async def command_stats_handler(message: Message):
    """
    Shows period selection keyboard for statistics report.
    """
    logger.info(f"User {message.from_user.id} requested /stats")
    keyboard = get_period_selection_keyboard("stats")
    await message.reply(
        "📅 **Выберите период для статистики:**",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )

# ===== HELPER FUNCTIONS FOR REPORT GENERATION =====

async def generate_excel_report(message: Message, start_date, end_date):
    """Generates Excel export for the specified date range."""
    import pandas as pd
    
    msg = await message.answer("⏳ Генерирую выгрузку...")
    
    try:
        tasks = await db_service.get_all_tasks(start_date, end_date)
        
        if not tasks:
            date_range = format_date_range(start_date, end_date)
            await msg.edit_text(f"📭 Нет данных за период **{date_range}**", parse_mode="Markdown")
            return
        
        df = pd.DataFrame(tasks)
        
        column_map = {
            'id': 'Номер диалога',
            'file_name': 'Номер аудиофайла',
            'result_text': 'Текст звонка',
            'resident_phrase': 'Фраза жителя',
            'refusal_marker': 'Маркер отказа',
            'accident_duration': 'Длительность аварии',
        }
        
        # Add operator phrase column (duplicate of refusal_marker for now)
        if 'refusal_marker' in df.columns:
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
        
        rename_map = {k: v for k, v in column_map.items() if k in df.columns}
        export_df = df.rename(columns=rename_map)
        
        for col in ordered_columns:
            if col not in export_df.columns:
                export_df[col] = ""
        
        export_df = export_df[ordered_columns]
        
        filename = f"export_{message.from_user.id}.xlsx"
        export_df.to_excel(filename, index=False)
        
        date_range = format_date_range(start_date, end_date)
        input_file = FSInputFile(filename)
        await message.answer_document(
            input_file,
            caption=f"📊 **Выгрузка за период {date_range}**\n📝 Записей: **{len(tasks)}**",
            parse_mode="Markdown"
        )
        
        os.remove(filename)
        await msg.delete()
        
        # Automatically generate stats report for the same period
        await generate_stats_report(message, start_date, end_date)
        
    except Exception as e:
        logger.error(f"Export error: {e}", exc_info=True)
        await msg.edit_text(f"❌ Ошибка выгрузки: {e}")


async def generate_stats_report(message: Message, start_date, end_date):
    """Generates statistics report for the specified date range."""
    status_msg = await message.answer("📊 Считаю статистику...")
    
    try:
        tasks = await db_service.get_all_tasks(start_date, end_date)
        
        if not tasks:
            date_range = format_date_range(start_date, end_date)
            await status_msg.edit_text(f"📭 Нет данных за период **{date_range}**", parse_mode="Markdown")
            return
        
        # Category counters
        cat_refusal = 0
        cat_no_brigade = 0
        cat_long = 0
        cat_redirect = 0
        
        # Street clustering
        street_map = {}
        relevant_count = 0
        
        for t in tasks:
            if t.get('is_relevant_hard'):
                relevant_count += 1
                
                if t.get('category_refusal_works'): cat_refusal += 1
                if t.get('category_no_brigade'): cat_no_brigade += 1
                if t.get('category_long_duration'): cat_long += 1
                if t.get('category_redirect'): cat_redirect += 1
                
                street = t.get('cleaned_street')
                house = t.get('cleaned_house')
                
                if street and house:
                    s_norm = street.strip().lower()
                    h_norm = house.strip().lower()
                    
                    if s_norm not in street_map:
                        street_map[s_norm] = {'name': street, 'houses': set()}
                    
                    street_map[s_norm]['houses'].add(h_norm)
        
        date_range = format_date_range(start_date, end_date)
        
        report = (
            f"📈 **Аналитическая Сводка**\n"
            f"📅 Период: **{date_range}**\n"
            f"Всего релевантных диалогов: **{relevant_count}**\n\n"
            f"🔍 **По категориям проблем:**\n"
            f"1. 🚫 Отказ в сроках: **{cat_refusal}**\n"
            f"2. 🚒 Нет бригады: **{cat_no_brigade}**\n"
            f"3. ⏳ Длительная (>24ч): **{cat_long}**\n"
            f"4. ↪️ Перенаправление: **{cat_redirect}**\n\n"
            f"🏘 **Проблемные улицы (2+ дома):**\n"
        )
        
        problem_streets = []
        for s_key, data in street_map.items():
            if len(data['houses']) >= 2:
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


# ===== CALLBACK HANDLER FOR PERIOD SELECTION =====

@router.callback_query(F.data.startswith("period:"))
async def period_callback_handler(callback: CallbackQuery, state: FSMContext):
    """Handles period selection from inline keyboard."""
    # Parse callback data: period:{command_type}:{period_type}
    parts = callback.data.split(":")
    command_type = parts[1]  # "export" or "stats"
    period_type = parts[2]   # "today", "yesterday", "week", "month", "custom"
    
    if period_type == "custom":
        # Enter FSM for custom date input
        if command_type == "export":
            await state.set_state(DateInputStates.waiting_export_date)
        else:
            await state.set_state(DateInputStates.waiting_stats_date)
        
        await callback.message.edit_text(
            "📝 **Введите период:**\n\n"
            "• Конкретный день: `DD.MM.YYYY`\n"
            "• Период: `с DD.MM.YYYY по DD.MM.YYYY`\n\n"
            "Например: `27.01.2026` или `с 20.01.2026 по 27.01.2026`",
            parse_mode="Markdown"
        )
    else:
        # Preset period
        start_date, end_date = get_preset_period(period_type)
        
        # Delete the keyboard message
        await callback.message.delete()
        
        # Generate report
        if command_type == "export":
            await generate_excel_report(callback.message, start_date, end_date)
        else:
            await generate_stats_report(callback.message, start_date, end_date)
    
    await callback.answer()


# ===== FSM HANDLERS FOR CUSTOM DATE INPUT =====

@router.message(DateInputStates.waiting_export_date)
async def export_custom_date_handler(message: Message, state: FSMContext):
    """Handles custom date input for /export command."""
    user_input = message.text.strip()
    
    # Try parsing as period
    period = parse_period(user_input)
    if period:
        start_date, end_date = period
        await state.clear()
        await generate_excel_report(message, start_date, end_date)
        return
    
    # Try parsing as single date
    date = parse_date(user_input)
    if date:
        # Full day period
        start_date = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = date.replace(hour=23, minute=59, second=59, microsecond=999999)
        await state.clear()
        await generate_excel_report(message, start_date, end_date)
        return
    
    # Validation error
    await message.reply(
        "❌ **Неверный формат даты.**\n\n"
        "Примеры корректного ввода:\n"
        "• `27.01.2026`\n"
        "• `с 20.01.2026 по 27.01.2026`",
        parse_mode="Markdown"
    )


@router.message(DateInputStates.waiting_stats_date)
async def stats_custom_date_handler(message: Message, state: FSMContext):
    """Handles custom date input for /stats command."""
    user_input = message.text.strip()
    
    # Try parsing as period
    period = parse_period(user_input)
    if period:
        start_date, end_date = period
        await state.clear()
        await generate_stats_report(message, start_date, end_date)
        return
    
    # Try parsing as single date
    date = parse_date(user_input)
    if date:
        # Full day period
        start_date = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = date.replace(hour=23, minute=59, second=59, microsecond=999999)
        await state.clear()
        await generate_stats_report(message, start_date, end_date)
        return
    
    # Validation error
    await message.reply(
        "❌ **Неверный формат даты.**\n\n"
        "Примеры корректного ввода:\n"
        "• `27.01.2026`\n"
        "• `с 20.01.2026 по 27.01.2026`",
        parse_mode="Markdown"
    )



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
        effective_content_type = ContentType.VOICE
    elif message.content_type == ContentType.AUDIO:
        file_id = message.audio.file_id
        original_name = message.audio.file_name or "audio.mp3"
        effective_content_type = ContentType.AUDIO
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
        if is_zip:
            effective_content_type = 'application/zip'
        elif is_rar:
            effective_content_type = 'application/x-rar-compressed'
        else:
            # Audio file sent as document
            effective_content_type = ContentType.DOCUMENT
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
