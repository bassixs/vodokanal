"""Keyboard utilities for report period selection."""
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


def get_period_selection_keyboard(command_type: str) -> InlineKeyboardMarkup:
    """
    Creates an inline keyboard for selecting report period.
    
    Args:
        command_type: Either "export" or "stats" to differentiate callback sources
        
    Returns:
        InlineKeyboardMarkup with period selection buttons
    """
    buttons = [
        [InlineKeyboardButton(
            text="📅 За сегодня",
            callback_data=f"period:{command_type}:today"
        )],
        [InlineKeyboardButton(
            text="📆 За вчера",
            callback_data=f"period:{command_type}:yesterday"
        )],
        [InlineKeyboardButton(
            text="📊 За последние 7 дней",
            callback_data=f"period:{command_type}:week"
        )],
        [InlineKeyboardButton(
            text="📈 За последние 30 дней",
            callback_data=f"period:{command_type}:month"
        )],
        [InlineKeyboardButton(
            text="✏️ Произвольный период",
            callback_data=f"period:{command_type}:custom"
        )]
    ]
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)
