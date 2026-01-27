"""FSM States for date input in report period selection."""
from aiogram.fsm.state import State, StatesGroup


class DateInputStates(StatesGroup):
    """States for handling custom date input."""
    waiting_export_date = State()  # Waiting for date input for /export command
    waiting_stats_date = State()   # Waiting for date input for /stats command
