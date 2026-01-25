import asyncio
import logging
import os
import re
import zipfile
from aiogram import Bot
from bot.services.database import DatabaseService
from bot.services.storage import YandexStorageService
from bot.services.speechkit import SpeechKitService
from bot.services.llm import YandexGPTService
from aiogram.types import FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton

logger = logging.getLogger("worker")

class BackgroundWorker:
    def __init__(self, bot: Bot, db: DatabaseService):
        self.bot = bot
        self.db = db
        self.storage_service = YandexStorageService()
        self.speechkit_service = SpeechKitService()
        self.llm_service = YandexGPTService()
        self.target_chat_id = os.getenv("TARGET_CHAT_ID")

    async def run(self):
        logger.info("Background worker started.")
        while True:
            try:
                task = await self.db.get_pending_task()
                if not task:
                    await asyncio.sleep(2) # Nothing to do, wait
                    continue
                
                logger.info(f"Processing task {task['id']} (Type: {task['file_type']})")
                await self.process_task(task)
                
            except Exception as e:
                logger.error(f"Worker loop error: {e}", exc_info=True)
                await asyncio.sleep(5)

    async def process_task(self, task):
        task_id = task['id']
        user_id = task['user_id']
        source_path = task['source_path'] 
        file_name = task['file_name']
        file_type = task['file_type']
        
        temp_filename = f"temp_{task_id}_{file_name}"

        try:
            # === ARCHIVE HANDLING ===
            if file_type == 'application/zip':
                await self.handle_archive(task_id, user_id, source_path, temp_filename)
                return

            # === AUDIO HANDLING ===
            # 1. Prepare File URL
            if source_path.startswith("http"):
                # Already an S3 URL (from archive unpacking)
                file_url = source_path
                # No download needed
            else:
                # Telegram File ID
                file = await self.bot.get_file(source_path)
                await self.bot.download_file(file.file_path, temp_filename)
                
                # Upload to S3
                object_name = f"queue/{task_id}/{file_name}"
                file_url = await self.storage_service.upload_file(temp_filename, object_name)
                
                if os.path.exists(temp_filename):
                    os.remove(temp_filename)

            # 3. SpeechKit
            # Notifying user (optional)
            # await self.bot.send_message(user_id, f"🔨 Задача #{task_id}: Начал распознавание...")
            
            operation_id = await self.speechkit_service.submit_recognition(file_url)
            text = await self.speechkit_service.wait_for_completion(operation_id)

            if not text:
                raise Exception("Empty result from SpeechKit")

            # === IVR CLEANING ===
            # Remove standard greeting "Здравствуйте... разговоры записываются"
            ivr_markers = ["разговоры записываются", "целях контроля качества"]
            text_lower = text.lower()
            
            for marker in ivr_markers:
                if marker in text_lower:
                    try:
                        # Find the end of the marker
                        idx = text_lower.rfind(marker) + len(marker)
                        clean_text = text[idx:].strip()
                        # If we stripped everything (empty), maybe keep original (unlikely)
                        if len(clean_text) > 5:
                            logger.info(f"Stripped IVR greeting. Removed {idx} chars.")
                            text = clean_text
                            break
                    except Exception as e:
                        logger.warning(f"Error stripping IVR: {e}")

            # 4. YandexGPT Analysis
            json_response_str = await self.llm_service.analyze_text(text)
            
            # Default values
            summary = "Ошибка парсинга"
            sentiment = "N/A"
            address = "Не определен"
            dialog_type = "Не определен"
            markers_str = ""
            is_relevant = False
            
            # Default values
            is_relevant_hard = False
            category_refusal_works = False
            category_no_brigade = False
            category_long_duration = False
            category_redirect = False
            cleaned_street = None
            cleaned_house = None
            resident_phrase = None
            accident_duration = None
            
            try:
                import json
                
                # Check and clean markdown fences if they leaked through
                cleaned_str = json_response_str.strip()
                if cleaned_str.startswith("```"):
                     # Remove first line if it's ```json or ```
                     cleaned_str = cleaned_str.split("\n", 1)[-1]
                if cleaned_str.endswith("```"):
                     cleaned_str = cleaned_str.rsplit("\n", 1)[0]
                
                data = json.loads(cleaned_str)
                
                summary = data.get("summary", "Без саммари")
                sentiment = data.get("sentiment", "N/A")
                address = data.get("address", "Не указан")
                dialog_type = data.get("dialog_type", "N/A")
                
                # New Analytics Fields
                is_relevant_hard = data.get("is_relevant_hard", False)
                resident_phrase = data.get("resident_phrase", "")
                accident_duration = data.get("accident_duration", "")
                
                stats = data.get("stats_categories", {})
                category_refusal_works = stats.get("refusal_deadline", False)
                category_no_brigade = stats.get("no_brigade", False)
                category_long_duration = stats.get("long_duration", False)
                category_redirect = stats.get("redirect_other_org", False)
                
                loc = data.get("location", {})
                cleaned_street = loc.get("street", "")
                cleaned_house = loc.get("house", "")
                
                markers = data.get("markers", [])
                if markers:
                    # Format as readable string for DB/Chat
                    m_list = [f"{m['marker_type']} ('{m['operator_phrase']}')" for m in markers]
                    markers_str = "; ".join(m_list)
                else:
                    markers_str = "Нет маркеров"
                
                # Use AI-cleaned dialogue if available, else usage raw
                final_transcript = data.get("cleaned_dialogue", text)
                if len(final_transcript) < 10: # Safety check
                    final_transcript = text
                    
            except json.JSONDecodeError:
                logger.error(f"Failed to parse JSON from LLM: {json_response_str}")
                summary = "Ошибка формата ответа нейросети"
                markers_str = json_response_str[:100]
                final_transcript = text

            # Sanitize types for DB
            if isinstance(final_transcript, list):
                final_transcript = "\n".join([str(x) for x in final_transcript])
            else:
                final_transcript = str(final_transcript)

            if isinstance(address, list):
                address = ", ".join([str(x) for x in address])
            else:
                address = str(address)
                
            summary = str(summary)
            sentiment = str(sentiment)
            dialog_type = str(dialog_type)

            
            # 5. Save to DB
            await self.db.complete_task(
                task_id=task_id, 
                summary=summary, 
                sentiment=sentiment, 
                full_text=final_transcript, # Saving the nice dialogue
                address=address,
                dialog_type=dialog_type,
                refusal_marker=markers_str,
                # New V3.1 args
                is_relevant_hard=is_relevant_hard,
                category_refusal_works=category_refusal_works,
                category_no_brigade=category_no_brigade,
                category_long_duration=category_long_duration,
                category_redirect=category_redirect,
                cleaned_street=cleaned_street,
                cleaned_house=cleaned_house,
                resident_phrase=resident_phrase,
                accident_duration=accident_duration
            )

            # 6. Send Report to Group
            if self.target_chat_id:
                await self.send_report(
                    task_id, file_name, summary, sentiment, 
                    address, dialog_type, markers_str, markers if 'markers' in locals() else [],
                    full_text=final_transcript
                )
            
            # Notify User
            await self.bot.send_message(user_id, f"✅ Задача #{task_id} выполнена! ({file_name})")

        except Exception as e:
            logger.error(f"Task {task_id} failed: {e}", exc_info=True)
            await self.db.fail_task(task_id, str(e))
            # await self.bot.send_message(user_id, f"❌ Задача #{task_id} упала с ошибкой: {e}")
            
        finally:
            if os.path.exists(temp_filename):
                os.remove(temp_filename)
            
            # Auto-cleanup S3 to save space (V5)
            # We delete the file from the queue after processing is done (success or fail)
            # NOTE: If we want to keep failed files for debug, we should add a check `if not error`.
            # But the user asked to "clean storage", so getting rid of processed files is key.
            # We construct the S3 key from the URL or known logic.
            # URL: https://storage.yandexcloud.net/BUCKET/queue/ID/NAME...
            # We can just remember object_name if we created it.
            
            if 'object_name' in locals() and object_name:
                 try:
                     await self.storage_service.delete_file(object_name)
                     logger.info(f"Auto-cleaned S3 file: {object_name}")
                 except Exception as ex:
                     logger.warning(f"Failed to auto-clean S3: {ex}")

    async def handle_archive(self, task_id, user_id, file_id, temp_zip_path):
        """Unpacks archive and creates sub-tasks."""
        extract_dir = f"temp_extract_{task_id}"
        os.makedirs(extract_dir, exist_ok=True)
        
        try:
            # Download Zip
            file = await self.bot.get_file(file_id)
            await self.bot.download_file(file.file_path, temp_zip_path)
            
            # Extract
            with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            
            # Iterate files
            files_found = 0
            for root, dirs, files in os.walk(extract_dir):
                for file_name in files:
                    # Filter audio extensions
                    if file_name.lower().endswith(('.mp3', '.ogg', '.wav', '.m4a', '.opus')):
                        file_path = os.path.join(root, file_name)
                        
                        # Upload to S3 immediately
                        object_name = f"archives/{task_id}/{file_name}"
                        s3_url = await self.storage_service.upload_file(file_path, object_name)
                        
                        # Create new task in DB
                        new_id = await self.db.add_task(
                            user_id=user_id,
                            file_type='audio_subtask',
                            source_path=s3_url,
                            file_name=file_name
                        )
                        files_found += 1
            
            await self.db.complete_task(task_id, f"Распаковано файлов: {files_found}", "N/A", "Archive processed")
            await self.bot.send_message(user_id, f"📦 Архив #{task_id} распакован. Добавлено {files_found} задач в очередь.")

        except Exception as e:
            raise e
        finally:
            # Cleanup
            if os.path.exists(temp_zip_path):
                os.remove(temp_zip_path)
            # Remove unpacked dir
            import shutil
            if os.path.exists(extract_dir):
                shutil.rmtree(extract_dir)

    async def send_report(self, task_id, file_name, summary, sentiment, address, dialog_type, markers_str, markers_list, full_text):
        import html
        
        # Format tags
        tag_sentiment = re.sub(r"[^a-zA-Zа-яА-Я0-9]", "", sentiment)
        
        # Markers Warning
        marker_alert = ""
        if markers_list:
            marker_alert = f"\n⚠️ <b>ВЫЯВЛЕНЫ НАРУШЕНИЯ ({len(markers_list)}):</b>"
            for m in markers_list:
                m_type = html.escape(m.get('marker_type', 'Marker'))
                m_phrase = html.escape(m.get('operator_phrase', ''))
                marker_alert += f"\n- 🔴 {m_type}: &quot;{m_phrase}&quot;"
        
        # Escape variables for HTML
        safe_file_name = html.escape(file_name)
        safe_address = html.escape(str(address))
        safe_dialog_type = html.escape(str(dialog_type))
        safe_summary = html.escape(str(summary))
        safe_sentiment = html.escape(str(tag_sentiment))

        # Create report text
        report = (
            f"📁 <b>Файл:</b> {safe_file_name} (#{task_id})\n"
            f"🏠 <b>Адрес:</b> {safe_address}\n"
            f"📞 <b>Тип:</b> {safe_dialog_type}\n"
            f"{marker_alert}\n\n"
            f"📋 <b>Саммари:</b> {safe_summary}\n"
            f"🎭 <b>Тон:</b> #{safe_sentiment}"
        )

        # Button
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Взять в работу", callback_data=f"take_task_{task_id}")]
        ])
        
        # Create transcript file
        transcript_filename = f"transcript_{task_id}.txt"
        with open(transcript_filename, "w", encoding="utf-8") as f:
            f.write(full_text)
        
        input_file = FSInputFile(transcript_filename)

        try:
             # Try sending as document with caption
             if len(report) < 1000:
                 await self.bot.send_document(
                     self.target_chat_id, 
                     document=input_file,
                     caption=report, 
                     reply_markup=kb, 
                     parse_mode="HTML"
                 )
             else:
                 # Too long for caption, send separate
                 await self.bot.send_document(self.target_chat_id, document=input_file)
                 await self.bot.send_message(self.target_chat_id, report, reply_markup=kb, parse_mode="HTML")
                 
        except Exception as e:
             logger.error(f"Report sending failed: {e}")
             await self.bot.send_message(self.target_chat_id, f"📁 #{task_id} Ошибка отправки отчета: {e}", reply_markup=kb)
        finally:
            if os.path.exists(transcript_filename):
                os.remove(transcript_filename)
