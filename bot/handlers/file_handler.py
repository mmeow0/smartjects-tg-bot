import io
import time
import asyncio
from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.exceptions import TelegramRetryAfter
from typing import List

from config import (
    MAX_FILE_SIZE,
    RateLimitConfig,
    Messages,
    ProcessingConfig,
    FEATURES
)
from utils.logging_config import get_logger

logger = get_logger(__name__)

router = Router(name="file_handler")

class ProcessingStates(StatesGroup):
    processing_file = State()

def check_user_access(user_id: int, allowed_users: List[int]) -> bool:
    """Check if user is allowed to use the bot"""
    if not allowed_users:  # If no restrictions, allow everyone
        return True
    return user_id in allowed_users

@router.message(F.document)
async def handle_document(message: types.Message, state: FSMContext, csv_processor, logo_matcher, allowed_users: List[int]):
    """Handle document uploads"""
    user_id = message.from_user.id

    if not check_user_access(user_id, allowed_users):
        await message.answer(Messages.ACCESS_DENIED)
        return

    document = message.document

    # Check if it's a CSV or XLSX file
    file_extension = document.file_name.lower()
    if not (file_extension.endswith('.csv') or file_extension.endswith('.xlsx')):
        await message.answer(Messages.FILE_WRONG_TYPE)
        return

    # Check file size
    if document.file_size > MAX_FILE_SIZE:
        await message.answer(
            Messages.FILE_TOO_LARGE.format(max_size=MAX_FILE_SIZE // (1024 * 1024))
        )
        return

    await state.set_state(ProcessingStates.processing_file)

    # Send initial message
    processing_message = await message.answer(
        "📥 Downloading file...",
        parse_mode="Markdown"
    )

    try:
        # Download file
        bot = message.bot
        file = await bot.get_file(document.file_id)
        file_data = await bot.download_file(file.file_path)

        # Read file content based on file type
        file_extension = document.file_name.lower()
        if file_extension.endswith('.xlsx'):
            # For XLSX files, keep as bytes
            file_content = file_data.read()
        else:
            # For CSV files, decode as text
            file_content = file_data.read().decode('utf-8')

        await processing_message.edit_text(
            f"📄 **Processing file:** {document.file_name}\n"
            f"📏 Size: {document.file_size:,} bytes\n\n"
            "🔄 Starting processing...",
            parse_mode="Markdown"
        )

        # Set logo matcher in CSV processor
        csv_processor.set_logo_matcher(logo_matcher)

        # Rate limiting for progress updates
        last_update_time = time.time()
        min_update_interval = RateLimitConfig.PROGRESS_UPDATE_INTERVAL

        # Progress callback with rate limiting
        async def progress_callback(current, total, title):
            nonlocal last_update_time
            current_time = time.time()

            # Update only if enough time has passed or it's the last record
            should_update = (current_time - last_update_time >= min_update_interval) or (current == total)

            if should_update:
                progress_percent = (current / total) * 100
                progress_bar = create_progress_bar(progress_percent)

                try:
                    await processing_message.edit_text(
                        f"📄 **Processing file:** {document.file_name}\n\n"
                        f"Progress: {progress_bar} {progress_percent:.1f}%\n"
                        f"({current}/{total})\n\n"
                        f"Current: {title[:50]}{'...' if len(title) > 50 else ''}",
                        parse_mode="Markdown"
                    )
                    last_update_time = current_time
                except TelegramRetryAfter as e:
                    # Handle rate limit by waiting the required time
                    logger.warning(f"Rate limit hit, waiting {e.retry_after} seconds")
                    await asyncio.sleep(e.retry_after)
                    # Try once more after waiting
                    try:
                        await processing_message.edit_text(
                            f"📄 **Processing file:** {document.file_name}\n\n"
                            f"Progress: {progress_bar} {progress_percent:.1f}%\n"
                            f"({current}/{total})\n\n"
                            f"Current: {title[:50]}{'...' if len(title) > 50 else ''}",
                            parse_mode="Markdown"
                        )
                        last_update_time = time.time()
                    except Exception as retry_error:
                        # If it still fails, just log and continue
                        logger.error(f"Failed to update progress after retry: {retry_error}")
                except Exception as e:
                    # Log other errors but continue processing
                    logger.error(f"Error updating progress: {e}")

        # Process file based on type
        if file_extension.endswith('.xlsx'):
            result = await csv_processor.process_xlsx_content(file_content, progress_callback)
        else:
            result = await csv_processor.process_csv_content(file_content, progress_callback)

        # Prepare detailed results
        stats = result['stats']
        results_list = result['results']

        # Create summary message
        summary_text = (
            f"✅ **Processing Complete!**\n\n"
            f"📄 File: {document.file_name}\n\n"
            + csv_processor.get_summary()
        )

        # Add teams sync information if any records were processed
        if stats['processed'] > 0:
            summary_text += "\n\n👥 **Teams Synchronization:**"
            summary_text += "\n🔄 Teams and relations have been synchronized"

        await processing_message.edit_text(summary_text, parse_mode="Markdown")

        # Send detailed results if there were any successes or errors
        if stats['processed'] > 0 or stats['errors'] > 0:
            await send_detailed_results(message, results_list, stats)

        # Clear state
        await state.clear()

    except TelegramRetryAfter as e:
        logger.error(f"Rate limit error: {e}")
        await asyncio.sleep(e.retry_after)
        try:
            await processing_message.edit_text(
                Messages.RATE_LIMIT_WARNING,
                parse_mode="Markdown"
            )
        except:
            pass
        await state.clear()
    except Exception as e:
        logger.error(f"Error processing file: {e}")
        try:
            await processing_message.edit_text(
                f"❌ **Error processing file**\n\n"
                f"File: {document.file_name}\n"
                f"Error: {str(e)}\n\n"
                "Please check the file format and try again.",
                parse_mode="Markdown"
            )
        except TelegramRetryAfter:
            # If we can't even send error message, just log it
            logger.error("Cannot send error message due to rate limits")
        await state.clear()

def create_progress_bar(percent: float, length: int = 20) -> str:
    """Create a text progress bar"""
    filled = int(length * percent / 100)
    bar = '█' * filled + '░' * (length - filled)
    return f"[{bar}]"

async def send_detailed_results(message: types.Message, results: List[dict], stats: dict):
    """Send detailed processing results"""

    # Group results by status
    successes = [r for r in results if r['status'] == 'success']
    errors = [r for r in results if r['status'] == 'error']

    # Send successful imports
    if successes:
        success_text = "✅ **Successfully imported:**\n\n"

        for idx, result in enumerate(successes[:20], 1):  # Limit to first 20
            title = result['title'][:50] + '...' if len(result['title']) > 50 else result['title']
            logo_info = f" 🎓" if result.get('logo_match') else ""
            success_text += f"{idx}. {title}{logo_info}\n"

            if result.get('industries'):
                success_text += f"   📊 Industries: {', '.join(result['industries'][:3])}\n"
            if result.get('audience'):
                success_text += f"   👥 Audience: {', '.join(result['audience'][:3])}\n"

        if len(successes) > 20:
            success_text += f"\n... and {len(successes) - 20} more"

        await message.answer(success_text, parse_mode="Markdown")

    # # Send errors if any
    # if errors:
    #     error_text = "❌ **Errors encountered:**\n\n"

    #     for idx, result in enumerate(errors[:10], 1):  # Limit to first 10
    #         title = result['title'][:50] + '...' if len(result['title']) > 50 else result['title']
    #         error = result.get('error', 'Unknown error')[:100]
    #         error_text += f"{idx}. {title}\n   Error: {error}\n\n"

    #     if len(errors) > 10:
    #         error_text += f"... and {len(errors) - 10} more errors"

    #     await message.answer(error_text, parse_mode="Markdown")

@router.message(ProcessingStates.processing_file)
async def handle_message_while_processing(message: types.Message):
    """Handle any messages while processing is in progress"""
    await message.answer(Messages.PROCESSING_IN_PROGRESS)
