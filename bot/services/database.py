import aiosqlite
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class DatabaseService:
    def __init__(self, db_path="bot_database.db"):
        self.db_path = db_path

    async def init_db(self):
        """Initializes the database table."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    file_type TEXT,
                    source_path TEXT, -- Telegram file_id OR S3 URL
                    file_name TEXT,
                    status TEXT DEFAULT 'queued', -- queued, processing, completed, error
                    result_summary TEXT,
                    result_sentiment TEXT,
                    result_text TEXT,
                    -- New Analytics Columns
                    address TEXT,
                    dialog_type TEXT,
                    refusal_marker TEXT, -- JSON or comma-separated list of markers found
                    
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Migration check (simple)
            try:
                await db.execute("ALTER TABLE tasks ADD COLUMN address TEXT")
            except Exception: pass
            
            try:
                await db.execute("ALTER TABLE tasks ADD COLUMN dialog_type TEXT")
            except Exception: pass
            
            try:
                await db.execute("ALTER TABLE tasks ADD COLUMN refusal_marker TEXT")
            except Exception: pass
            
            # V3.1 Migrations
            try:
                await db.execute("ALTER TABLE tasks ADD COLUMN is_relevant_hard BOOLEAN DEFAULT 0")
            except Exception: pass
            
            try:
                await db.execute("ALTER TABLE tasks ADD COLUMN category_refusal_works BOOLEAN DEFAULT 0")
            except Exception: pass
            
            try:
                await db.execute("ALTER TABLE tasks ADD COLUMN category_no_brigade BOOLEAN DEFAULT 0")
            except Exception: pass
                
            try:
                await db.execute("ALTER TABLE tasks ADD COLUMN category_long_duration BOOLEAN DEFAULT 0")
            except Exception: pass
                
            try:
                await db.execute("ALTER TABLE tasks ADD COLUMN category_redirect BOOLEAN DEFAULT 0")
            except Exception: pass
            
            try:
                await db.execute("ALTER TABLE tasks ADD COLUMN cleaned_street TEXT")
            except Exception: pass
                
            try:
                await db.execute("ALTER TABLE tasks ADD COLUMN cleaned_house TEXT")
            except Exception: pass
            
            await db.commit()
            logger.info("Database initialized.")

    async def add_task(self, user_id, file_type, source_path, file_name):
        """Adds a new task to the queue."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "INSERT INTO tasks (user_id, file_type, source_path, file_name) VALUES (?, ?, ?, ?)",
                (user_id, file_type, source_path, file_name)
            )
            await db.commit()
            return cursor.lastrowid

    async def get_pending_task(self):
        """Retrieves the oldest pending task and marks it as processing."""
        async with aiosqlite.connect(self.db_path) as db:
            # Simple transaction to lock the task
            # SQLite doesn't support SELECT FOR UPDATE properly like PG, but for this scale it's fine.
            # We will mark it as processing immediately.
            async with db.execute("SELECT id, user_id, file_type, source_path, file_name FROM tasks WHERE status = 'queued' ORDER BY id ASC LIMIT 1") as cursor:
                task = await cursor.fetchone()
            
            if task:
                task_id = task[0]
                await db.execute("UPDATE tasks SET status = 'processing' WHERE id = ?", (task_id,))
                await db.commit()
                return {
                    "id": task[0],
                    "user_id": task[1],
                    "file_type": task[2],
                    "source_path": task[3],
                    "file_name": task[4]
                }
            return None

    async def get_all_tasks(self):
        """Retrieves all tasks for export."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM tasks ORDER BY id DESC") as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def complete_task(
        self, task_id, summary, sentiment, full_text, 
        address=None, dialog_type=None, refusal_marker=None,
        is_relevant_hard=False,
        category_refusal_works=False,
        category_no_brigade=False,
        category_long_duration=False,
        category_redirect=False,
        cleaned_street=None,
        cleaned_house=None
    ):
        """Marks task as completed with results."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                UPDATE tasks SET 
                    status = 'completed', 
                    result_summary = ?, 
                    result_sentiment = ?, 
                    result_text = ?, 
                    address = ?, 
                    dialog_type = ?, 
                    refusal_marker = ?,
                    is_relevant_hard = ?,
                    category_refusal_works = ?,
                    category_no_brigade = ?,
                    category_long_duration = ?,
                    category_redirect = ?,
                    cleaned_street = ?,
                    cleaned_house = ?
                WHERE id = ?
                """,
                (
                    summary, sentiment, full_text, address, dialog_type, refusal_marker,
                    is_relevant_hard, category_refusal_works, category_no_brigade, category_long_duration, category_redirect,
                    cleaned_street, cleaned_house,
                    task_id
                )
            )
            await db.commit()

    async def fail_task(self, task_id, error_message):
        """Marks task as error."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE tasks SET status = 'error', error_message = ? WHERE id = ?",
                (error_message, task_id)
            )
            await db.commit()
