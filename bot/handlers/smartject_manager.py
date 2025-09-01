from aiogram import Router, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from typing import List, Dict, Optional
import json

from services.supabase_client import SupabaseClient
from utils.logging_config import get_logger
from config import ALLOWED_USERS

logger = get_logger(__name__)

router = Router(name="smartject_manager")

# States for managing smartjects
class SmartjectStates(StatesGroup):
    searching = State()
    viewing = State()
    editing = State()
    editing_field = State()
    confirming_delete = State()
    selecting_tags = State()

def check_user_access(user_id: int, allowed_users: List[int]) -> bool:
    """Check if user is allowed to use management functions"""
    if not allowed_users:  # If no restrictions, allow everyone
        return True
    return user_id in allowed_users

def create_search_results_keyboard(smartjects: List[Dict]) -> InlineKeyboardMarkup:
    """Create keyboard with search results"""
    keyboard = []

    for smartject in smartjects[:5]:  # Limit to 5 results
        title = smartject['title']
        if len(title) > 30:
            title = title[:27] + "..."

        keyboard.append([
            InlineKeyboardButton(
                text=f"📄 {title}",
                callback_data=f"view_{smartject['id']}"
            )
        ])

    keyboard.append([
        InlineKeyboardButton(text="❌ Cancel", callback_data="cancel_search")
    ])

    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def create_smartject_menu_keyboard(smartject_id: str) -> InlineKeyboardMarkup:
    """Create menu keyboard for smartject actions"""
    keyboard = [
        [
            InlineKeyboardButton(text="✏️ Edit", callback_data=f"edit_{smartject_id}"),
            InlineKeyboardButton(text="🗑️ Delete", callback_data=f"delete_{smartject_id}")
        ],
        [
            InlineKeyboardButton(text="🔙 Back to Search", callback_data="back_to_search"),
            InlineKeyboardButton(text="❌ Close", callback_data="close_menu")
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def create_edit_menu_keyboard(smartject_id: str) -> InlineKeyboardMarkup:
    """Create keyboard for editing smartject fields"""
    keyboard = [
        [InlineKeyboardButton(text="📝 Title", callback_data=f"edit_title_{smartject_id}")],
        [InlineKeyboardButton(text="🎯 Mission", callback_data=f"edit_mission_{smartject_id}")],
        [InlineKeyboardButton(text="❓ Problematics", callback_data=f"edit_problematics_{smartject_id}")],
        [InlineKeyboardButton(text="🔍 Scope", callback_data=f"edit_scope_{smartject_id}")],
        [InlineKeyboardButton(text="👥 Audience", callback_data=f"edit_audience_{smartject_id}")],
        [InlineKeyboardButton(text="⚙️ How it Works", callback_data=f"edit_how_it_works_{smartject_id}")],
        [InlineKeyboardButton(text="🏗️ Architecture", callback_data=f"edit_architecture_{smartject_id}")],
        [InlineKeyboardButton(text="💡 Innovation", callback_data=f"edit_innovation_{smartject_id}")],
        [InlineKeyboardButton(text="📋 Use Case", callback_data=f"edit_use_case_{smartject_id}")],
        [InlineKeyboardButton(text="🏢 Industries", callback_data=f"edit_industries_{smartject_id}")],
        [InlineKeyboardButton(text="🎓 Teams", callback_data=f"edit_teams_{smartject_id}")],
        [InlineKeyboardButton(text="🔗 Image URL", callback_data=f"edit_image_url_{smartject_id}")],
        [
            InlineKeyboardButton(text="🔙 Back", callback_data=f"view_{smartject_id}"),
            InlineKeyboardButton(text="❌ Cancel", callback_data="cancel_edit")
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def create_delete_confirmation_keyboard(smartject_id: str) -> InlineKeyboardMarkup:
    """Create confirmation keyboard for deletion"""
    keyboard = [
        [
            InlineKeyboardButton(text="✅ Yes, Delete", callback_data=f"confirm_delete_{smartject_id}"),
            InlineKeyboardButton(text="❌ Cancel", callback_data=f"cancel_delete_{smartject_id}")
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def format_smartject_details(smartject: Dict) -> str:
    """Format smartject details for display"""
    text = f"📄 **{smartject['title']}**\n\n"

    if smartject.get('mission'):
        text += f"**🎯 Mission:**\n{smartject['mission'][:200]}{'...' if len(smartject.get('mission', '')) > 200 else ''}\n\n"

    if smartject.get('problematics'):
        text += f"**❓ Problematics:**\n{smartject['problematics'][:200]}{'...' if len(smartject.get('problematics', '')) > 200 else ''}\n\n"

    if smartject.get('scope'):
        text += f"**🔍 Scope:**\n{smartject['scope'][:200]}{'...' if len(smartject.get('scope', '')) > 200 else ''}\n\n"

    # Add tags
    if smartject.get('industries'):
        industries = [ind['name'] for ind in smartject['industries']]
        text += f"**🏢 Industries:** {', '.join(industries[:3])}\n"

    if smartject.get('audience_list'):
        audience = [aud['name'] for aud in smartject['audience_list']]
        text += f"**👥 Audience:** {', '.join(audience[:3])}\n"

    if smartject.get('teams_list'):
        teams = [team['name'] for team in smartject['teams_list']]
        text += f"**🎓 Teams:** {', '.join(teams[:3])}\n"

    # Add metadata
    if smartject.get('created_at'):
        text += f"\n**📅 Created:** {smartject['created_at'][:10]}"

    return text

# Command handlers
@router.message(Command("search"))
async def cmd_search(message: types.Message, state: FSMContext, allowed_users: List[int]):
    """Handle /search command"""
    user_id = message.from_user.id

    if not check_user_access(user_id, allowed_users):
        await message.answer("❌ You don't have permission to manage smartjects.")
        return

    await state.set_state(SmartjectStates.searching)
    await message.answer(
        "🔍 **Search for a Smartject**\n\n"
        "Enter part of the smartject title to search:",
        parse_mode="Markdown"
    )

@router.message(Command("manage"))
async def cmd_manage(message: types.Message, allowed_users: List[int]):
    """Handle /manage command"""
    user_id = message.from_user.id

    if not check_user_access(user_id, allowed_users):
        await message.answer("❌ You don't have permission to manage smartjects.")
        return

    await message.answer(
        "📊 **Smartject Management**\n\n"
        "Available commands:\n"
        "• /search - Search for smartjects by title\n"
        "• /cancel - Cancel current operation\n\n"
        "To edit or delete a smartject, first search for it by title.",
        parse_mode="Markdown"
    )

@router.message(Command("cancel"))
async def cmd_cancel(message: types.Message, state: FSMContext):
    """Handle /cancel command"""
    await state.clear()
    await message.answer("Operation cancelled.")

# State handlers
@router.message(SmartjectStates.searching)
async def handle_search_query(message: types.Message, state: FSMContext, supabase_client: SupabaseClient):
    """Handle search query input"""
    query = message.text.strip()

    if len(query) < 2:
        await message.answer("Please enter at least 2 characters to search.")
        return

    await message.answer("🔍 Searching...")

    # Search for smartjects
    smartjects = supabase_client.search_smartjects_by_title(query)

    if not smartjects:
        await message.answer(
            f"No smartjects found matching '{query}'.\n"
            "Try a different search term or /cancel to stop."
        )
        return

    # Store search results in state
    await state.update_data(search_results=smartjects)

    # Show results with inline keyboard
    keyboard = create_search_results_keyboard(smartjects)
    await message.answer(
        f"Found {len(smartjects)} smartject(s) matching '{query}':\n"
        "Select one to view details:",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )
    await state.set_state(SmartjectStates.viewing)

@router.message(SmartjectStates.editing_field)
async def handle_field_edit(message: types.Message, state: FSMContext, supabase_client: SupabaseClient):
    """Handle field value input for editing"""
    new_value = message.text.strip()

    # Get state data
    data = await state.get_data()
    smartject_id = data.get('editing_smartject_id')
    field_name = data.get('editing_field')

    if not smartject_id or not field_name:
        await message.answer("Error: Missing smartject or field information.")
        await state.clear()
        return

    # Special handling for teams (convert to list)
    if field_name == 'teams':
        new_value = [team.strip() for team in new_value.split(',')]

    # Update the smartject
    update_data = {field_name: new_value}
    success = supabase_client.update_smartject(smartject_id, update_data)

    if success:
        await message.answer(
            f"✅ Successfully updated {field_name}!",
            parse_mode="Markdown"
        )

        # Show edit menu again
        keyboard = create_edit_menu_keyboard(smartject_id)
        await message.answer(
            "Select another field to edit or go back:",
            reply_markup=keyboard
        )
        await state.set_state(SmartjectStates.editing)
    else:
        await message.answer(
            f"❌ Failed to update {field_name}. Please try again.",
            parse_mode="Markdown"
        )

# Callback query handlers
@router.callback_query(F.data.startswith("view_"))
async def handle_view_smartject(callback: CallbackQuery, state: FSMContext, supabase_client: SupabaseClient):
    """Handle viewing smartject details"""
    smartject_id = callback.data.replace("view_", "")

    # Get full smartject details
    smartject = supabase_client.get_smartject_details(smartject_id)

    if not smartject:
        await callback.answer("Smartject not found!")
        return

    # Format and display details
    text = format_smartject_details(smartject)
    keyboard = create_smartject_menu_keyboard(smartject_id)

    await callback.message.edit_text(
        text,
        parse_mode="Markdown",
        reply_markup=keyboard
    )
    await callback.answer()

@router.callback_query(F.data.startswith("edit_"))
async def handle_edit_smartject(callback: CallbackQuery, state: FSMContext):
    """Handle edit smartject action"""
    data_parts = callback.data.split("_", 1)

    if len(data_parts) == 2 and data_parts[1] and not callback.data.startswith("edit_title_") \
            and not callback.data.startswith("edit_mission_") and not callback.data.startswith("edit_problematics_") \
            and not callback.data.startswith("edit_scope_") and not callback.data.startswith("edit_audience_") \
            and not callback.data.startswith("edit_how_it_works_") and not callback.data.startswith("edit_architecture_") \
            and not callback.data.startswith("edit_innovation_") and not callback.data.startswith("edit_use_case_") \
            and not callback.data.startswith("edit_industries_") and not callback.data.startswith("edit_teams_") \
            and not callback.data.startswith("edit_image_url_"):
        # Show edit menu
        smartject_id = data_parts[1]
        keyboard = create_edit_menu_keyboard(smartject_id)

        await callback.message.edit_text(
            "Select a field to edit:",
            reply_markup=keyboard
        )
        await state.set_state(SmartjectStates.editing)
        await callback.answer()
    else:
        # Handle specific field edit
        field_mapping = {
            'edit_title_': 'title',
            'edit_mission_': 'mission',
            'edit_problematics_': 'problematics',
            'edit_scope_': 'scope',
            'edit_audience_': 'audience',
            'edit_how_it_works_': 'how_it_works',
            'edit_architecture_': 'architecture',
            'edit_innovation_': 'innovation',
            'edit_use_case_': 'use_case',
            'edit_industries_': 'industries',
            'edit_teams_': 'teams',
            'edit_image_url_': 'image_url'
        }

        field_name = None
        smartject_id = None

        for prefix, field in field_mapping.items():
            if callback.data.startswith(prefix):
                field_name = field
                smartject_id = callback.data.replace(prefix, "")
                break

        if field_name and smartject_id:
            await state.update_data(
                editing_smartject_id=smartject_id,
                editing_field=field_name
            )

            # Special instructions for different fields
            instructions = {
                'teams': "Enter team names separated by commas:",
                'industries': "Note: Industries need to be updated with valid IDs. Contact admin for complex edits.",
                'audience': "Note: Audience tags need to be updated with valid IDs. Contact admin for complex edits."
            }

            instruction = instructions.get(field_name, f"Enter new value for {field_name}:")

            await callback.message.edit_text(
                f"**Editing {field_name}**\n\n{instruction}\n\nSend /cancel to abort.",
                parse_mode="Markdown"
            )
            await state.set_state(SmartjectStates.editing_field)
            await callback.answer()

@router.callback_query(F.data.startswith("delete_"))
async def handle_delete_smartject(callback: CallbackQuery, state: FSMContext, supabase_client: SupabaseClient):
    """Handle delete smartject action"""
    smartject_id = callback.data.replace("delete_", "")

    # Get smartject details for confirmation
    smartject = supabase_client.get_smartject_details(smartject_id)

    if not smartject:
        await callback.answer("Smartject not found!")
        return

    # Show confirmation dialog
    keyboard = create_delete_confirmation_keyboard(smartject_id)
    await callback.message.edit_text(
        f"⚠️ **Confirm Deletion**\n\n"
        f"Are you sure you want to delete:\n"
        f"**{smartject['title']}**?\n\n"
        f"This action cannot be undone!",
        parse_mode="Markdown",
        reply_markup=keyboard
    )
    await state.set_state(SmartjectStates.confirming_delete)
    await callback.answer()

@router.callback_query(F.data.startswith("confirm_delete_"))
async def handle_confirm_delete(callback: CallbackQuery, state: FSMContext, supabase_client: SupabaseClient):
    """Handle delete confirmation"""
    smartject_id = callback.data.replace("confirm_delete_", "")

    # Delete the smartject
    success = supabase_client.delete_smartject(smartject_id)

    if success:
        await callback.message.edit_text(
            "✅ Smartject successfully deleted!",
            parse_mode="Markdown"
        )
    else:
        await callback.message.edit_text(
            "❌ Failed to delete smartject. Please try again.",
            parse_mode="Markdown"
        )

    await state.clear()
    await callback.answer()

@router.callback_query(F.data.startswith("cancel_delete_"))
async def handle_cancel_delete(callback: CallbackQuery, state: FSMContext):
    """Handle delete cancellation"""
    smartject_id = callback.data.replace("cancel_delete_", "")

    # Go back to smartject view
    keyboard = create_smartject_menu_keyboard(smartject_id)
    await callback.message.edit_text(
        "Deletion cancelled.",
        reply_markup=keyboard
    )
    await state.set_state(SmartjectStates.viewing)
    await callback.answer()

@router.callback_query(F.data == "back_to_search")
async def handle_back_to_search(callback: CallbackQuery, state: FSMContext):
    """Handle going back to search"""
    await callback.message.edit_text(
        "🔍 Enter part of the smartject title to search:",
        parse_mode="Markdown"
    )
    await state.set_state(SmartjectStates.searching)
    await callback.answer()

@router.callback_query(F.data == "cancel_search")
@router.callback_query(F.data == "cancel_edit")
@router.callback_query(F.data == "close_menu")
async def handle_cancel(callback: CallbackQuery, state: FSMContext):
    """Handle cancellation callbacks"""
    await callback.message.edit_text("Operation cancelled.")
    await state.clear()
    await callback.answer()
