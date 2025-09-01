
from aiogram import Router, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from typing import List, Optional
from utils.logging_config import get_logger

logger = get_logger(__name__)

router = Router(name="command_handler")

def check_user_access(user_id: int, allowed_users: List[int]) -> bool:
    """Check if user is allowed to use the bot"""
    if not allowed_users:  # If no restrictions, allow everyone
        return True
    return user_id in allowed_users

@router.message(Command("start"))
async def cmd_start(message: types.Message, allowed_users: List[int]):
    """Handle /start command"""
    user_id = message.from_user.id

    if not check_user_access(user_id, allowed_users):
        await message.answer(
            "❌ Access denied. You are not authorized to use this bot."
        )
        return

    welcome_text = (
        "👋 Welcome to Smartjects Processor Bot!\n\n"
        "This bot helps you import and manage smartjects in Supabase "
        "with automatic university logo matching.\n\n"
        "📝 **How to use:**\n"
        "1. Send me a CSV or XLSX file with smartjects data\n"
        "   - For XLSX: data should be in 'smartjects' sheet\n"
        "2. I'll process it and add new smartjects to the database\n"
        "3. I'll automatically match university logos where possible\n\n"
        "📊 **Management:**\n"
        "• Use /search to find and edit/delete existing smartjects\n"
        "• Use /manage for management options\n\n"
        "Use /help for more information."
    )

    await message.answer(welcome_text, parse_mode="Markdown")

@router.message(Command("help"))
async def cmd_help(message: types.Message, allowed_users: List[int]):
    """Handle /help command"""
    user_id = message.from_user.id

    if not check_user_access(user_id, allowed_users):
        await message.answer(
            "❌ Access denied. You are not authorized to use this bot."
        )
        return

    help_text = (
        "📚 **Smartjects Processor Bot Help**\n\n"
        "**📁 File Processing Commands:**\n"
        "/start - Start the bot and see welcome message\n"
        "/help - Show this help message\n"
        "/status - Check bot status and database connection\n"
        "/logos - Show available universities with logos\n"
        "/stats - Show processing statistics\n"
        "/sync_teams - Manually sync teams from smartjects\n"
        "/check_duplicates - Check for duplicate smartjects in database\n"
        "/update_logos - Re-process logos for existing smartjects\n\n"
        "**📊 Smartject Management Commands:**\n"
        "/manage - Show management options\n"
        "/search - Search for smartjects by title\n"
        "/cancel - Cancel current operation\n\n"
        "**📄 File Formats:**\n"
        "• **CSV files** - Direct CSV format\n"
        "• **XLSX files** - Excel files with 'smartjects' sheet\n\n"
        "**CSV/XLSX Column Format:**\n"
        "Your CSV file should contain the following columns:\n"
        "- `name` - Smartject title (required)\n"
        "- `mission` - Mission statement\n"
        "- `problematics` - Problem description\n"
        "- `scope` - Scope of the project\n"
        "- `audience` - Target audience\n"
        "- `how it works` - How it works\n"
        "- `architecture` - Technical architecture\n"
        "- `innovation` - Innovation aspects\n"
        "- `use case` - Use cases\n"
        "- `industries` - Industries (comma-separated)\n"
        "- `functions` - Business functions (comma-separated)\n"
        "- `team` - Team members/universities (comma-separated)\n"
        "- `link` - Research paper link\n"
        "- `publish_date` - Publication date\n"
        "- `summarized` - Summary status\n\n"
        "**Notes:**\n"
        "- Records marked as 'NO (not relevant)' will be skipped\n"
        "- Existing smartjects (by title) will be skipped\n"
        "- University logos will be matched automatically from the team field"
    )

    await message.answer(help_text, parse_mode="Markdown")

@router.message(Command("status"))
async def cmd_status(message: types.Message, allowed_users: List[int], csv_processor, logo_matcher):
    """Handle /status command - check bot and database status"""
    user_id = message.from_user.id

    if not check_user_access(user_id, allowed_users):
        await message.answer(
            "❌ Access denied. You are not authorized to use this bot."
        )
        return

    status_message = await message.answer("🔍 Checking status...")

    try:
        # Check database connection
        existing_titles = csv_processor.supabase.fetch_existing_titles()

        # Get reference data counts
        industries_count = len(csv_processor.industries)
        audience_count = len(csv_processor.audience)
        functions_count = len(csv_processor.business_functions)

        # Get logos count
        logos_count = len(set(logo_matcher.logos_dict.values())) if logo_matcher.logos_dict else 0

        status_text = (
            "✅ **Bot Status: Online**\n\n"
            "📊 **Database Status:**\n"
            f"- Smartjects: {len(existing_titles)}\n"
            f"- Industries: {industries_count}\n"
            f"- Audience types: {audience_count}\n"
            f"- Business functions: {functions_count}\n\n"
            f"🎓 **Logo Matcher:**\n"
            f"- Universities with logos: {logos_count}\n"
            f"- Logos file: {'✅ Loaded' if logos_count > 0 else '❌ Not loaded'}"
        )

        await status_message.edit_text(status_text, parse_mode="Markdown")

    except Exception as e:
        logger.error(f"Error checking status: {e}")
        await status_message.edit_text(
            "❌ **Error checking status**\n"
            f"Error: {str(e)}"
        )

@router.message(Command("logos"))
async def cmd_logos(message: types.Message, allowed_users: List[int], logo_matcher):
    """Handle /logos command - show available universities with logos"""
    user_id = message.from_user.id

    if not check_user_access(user_id, allowed_users):
        await message.answer(
            "❌ Access denied. You are not authorized to use this bot."
        )
        return

    universities = logo_matcher.get_available_universities()

    if not universities:
        await message.answer(
            "❌ No universities with logos found.\n"
            "Make sure the logos CSV file is properly loaded."
        )
        return

    # Split into chunks to avoid too long messages
    chunk_size = 50
    total = len(universities)

    await message.answer(
        f"🎓 **Available Universities with Logos** ({total} total):",
        parse_mode="Markdown"
    )

    for i in range(0, total, chunk_size):
        chunk = universities[i:i + chunk_size]
        text = "\n".join(f"• {uni}" for uni in chunk)
        await message.answer(text)

@router.message(Command("stats"))
async def cmd_stats(message: types.Message, allowed_users: List[int], csv_processor):
    """Handle /stats command - show processing statistics"""
    user_id = message.from_user.id

    if not check_user_access(user_id, allowed_users):
        await message.answer(
            "❌ Access denied. You are not authorized to use this bot."
        )
        return

    if csv_processor.stats['total'] == 0:
        await message.answer(
            "📊 No processing statistics available yet.\n"
            "Send a CSV file to start processing smartjects."
        )
        return

    stats_text = csv_processor.get_summary()
    await message.answer(stats_text)

@router.message(Command("sync_teams"))
async def cmd_sync_teams(message: types.Message, allowed_users: List[int], csv_processor):
    """Handle /sync_teams command - manually sync teams from smartjects"""
    user_id = message.from_user.id

    if not check_user_access(user_id, allowed_users):
        await message.answer(
            "❌ Access denied. You are not authorized to use this bot."
        )
        return

    sync_message = await message.answer("🔄 Starting teams synchronization...")

    try:
        # Run batch teams synchronization
        stats = csv_processor.supabase.batch_sync_all_teams()

        result_text = (
            "✅ **Teams Synchronization Complete**\n\n"
            f"👥 New teams created: {stats['new_teams']}\n"
            f"🔗 New relations created: {stats['new_relations']}\n"
        )

        if stats['errors'] > 0:
            result_text += f"❌ Errors: {stats['errors']}\n"

        await sync_message.edit_text(result_text, parse_mode="Markdown")

    except Exception as e:
        logger.error(f"Error during teams sync: {e}")
        await sync_message.edit_text(
            f"❌ **Error during teams synchronization**\n\n"
            f"Error: {str(e)}",
            parse_mode="Markdown"
        )

@router.message(Command("check_duplicates"))
async def cmd_check_duplicates(message: types.Message, allowed_users: List[int], csv_processor):
    """Handle /check_duplicates command - check for duplicate smartjects"""
    user_id = message.from_user.id

    if not check_user_access(user_id, allowed_users):
        await message.answer(
            "❌ Access denied. You are not authorized to use this bot."
        )
        return

    check_message = await message.answer("🔍 Checking for duplicates...")

    try:
        # Get all smartjects with normalized titles
        all_smartjects = csv_processor.supabase.fetch_all_smartjects()

        # Group by normalized title
        title_groups = {}
        for smartject in all_smartjects:
            title = smartject.get('title', '')
            if title:
                normalized = title.strip().lower()
                if normalized not in title_groups:
                    title_groups[normalized] = []
                title_groups[normalized].append({
                    'id': smartject.get('id'),
                    'title': title,
                    'created_at': smartject.get('created_at', 'Unknown')
                })

        # Find duplicates
        duplicates = {k: v for k, v in title_groups.items() if len(v) > 1}

        if not duplicates:
            await check_message.edit_text(
                "✅ **No duplicates found!**\n\n"
                "All smartjects have unique titles.",
                parse_mode="Markdown"
            )
            return

        # Format results
        duplicate_count = sum(len(v) - 1 for v in duplicates.values())
        unique_titles = len(duplicates)

        result_text = (
            f"⚠️ **Duplicates Found**\n\n"
            f"📊 Total duplicate entries: {duplicate_count}\n"
            f"📝 Unique titles with duplicates: {unique_titles}\n\n"
            "**First 10 duplicate groups:**\n\n"
        )

        for idx, (normalized, entries) in enumerate(list(duplicates.items())[:10], 1):
            result_text += f"{idx}. **\"{entries[0]['title']}\"** ({len(entries)} copies)\n"
            for entry in entries[:3]:  # Show max 3 entries per group
                result_text += f"   • ID: `{entry['id'][:8]}...`\n"
            if len(entries) > 3:
                result_text += f"   • ... and {len(entries) - 3} more\n"
            result_text += "\n"

        if len(duplicates) > 10:
            result_text += f"... and {len(duplicates) - 10} more duplicate groups\n"

        result_text += "\n💡 **Tip:** Run the SQL migration to add a unique constraint and prevent future duplicates."

        await check_message.edit_text(result_text, parse_mode="Markdown")

    except Exception as e:
        logger.error(f"Error checking duplicates: {e}")
        await check_message.edit_text(
            f"❌ **Error checking duplicates**\n\n"
            f"Error: {str(e)}",
            parse_mode="Markdown"
        )

@router.message(Command("update_logos"))
async def cmd_update_logos(message: types.Message, allowed_users: List[int], logo_matcher):
    """Handle /update_logos command - re-process logos for existing smartjects"""
    user_id = message.from_user.id

    if not check_user_access(user_id, allowed_users):
        await message.answer(
            "❌ Access denied. You are not authorized to use this bot."
        )
        return

    update_message = await message.answer("🔄 Starting logo update process...")

    try:
        # Run logo update for existing smartjects
        stats = logo_matcher.update_existing_smartject_logos(dry_run=False)

        result_text = (
            "✅ **Logo Update Complete**\n\n"
            f"📊 Total smartjects: {stats['total_smartjects']}\n"
            f"👥 With teams: {stats['with_teams']}\n"
            f"🎓 Found matches: {stats['found_matches']}\n"
            f"✅ Updated: {stats['updated']}\n"
            f"⏭️  Already correct: {stats['already_correct']}\n"
        )

        if stats['errors'] > 0:
            result_text += f"❌ Errors: {stats['errors']}\n"

        result_text += "\n💡 **Note:** Only smartjects with team data can have logos matched."

        await update_message.edit_text(result_text, parse_mode="Markdown")

    except Exception as e:
        logger.error(f"Error updating logos: {e}")
        await update_message.edit_text(
            f"❌ **Error updating logos**\n\n"
            f"Error: {str(e)}",
            parse_mode="Markdown"
        )

@router.message(Command("cancel"))
async def cmd_cancel(message: types.Message, state: FSMContext):
    """Handle /cancel command - cancel any ongoing operation"""
    current_state = await state.get_state()

    if current_state is None:
        await message.answer("Nothing to cancel.")
        return

    await state.clear()
    await message.answer(
        "Operation cancelled. You can start over by sending a new CSV file.",
        reply_markup=types.ReplyKeyboardRemove()
    )
