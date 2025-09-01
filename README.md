# Smartjects Telegram Bot

A Telegram bot for importing smartjects data from CSV files into Supabase with automatic university logo matching.

## Features

- 📥 Import smartjects from CSV files via Telegram
- 🎓 Automatic university logo matching based on team members (or null if not found)
- ✅ Duplicate detection (skips existing smartjects)
- 📊 Progress tracking during import
- 🔍 Detailed import results and statistics
- 🛡️ Optional user access control
- 📈 Real-time processing status updates
- 👥 Automatic teams synchronization

## Prerequisites

- Python 3.8+
- Telegram Bot Token (from [@BotFather](https://t.me/botfather))
- Supabase instance (local or cloud)
- University logos CSV file (optional, for logo matching)

## Installation

1. Clone the repository:
```bash
cd smartjects-tg-bot
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Copy the environment file:
```bash
cp .env.example .env
```

4. Edit `.env` file and add your configuration:
```env
# Telegram Bot Configuration
BOT_TOKEN=your_telegram_bot_token_here

# Supabase Configuration
SUPABASE_URL=http://127.0.0.1:54321
SUPABASE_KEY=your_supabase_key_here

# Optional: Comma-separated list of allowed Telegram user IDs
# Leave empty to allow all users
ALLOWED_USERS=123456789,987654321
```

## Running the Bot

Start the bot:
```bash
python bot/main.py
```

The bot will start polling for messages and confirm successful startup.

## Usage

1. Start a conversation with your bot on Telegram
2. Send `/start` to see the welcome message
3. Send a CSV file with smartjects data
4. The bot will process the file and provide:
   - Real-time progress updates
   - Summary of processed records
   - Detailed results for successful imports and errors

## Bot Commands

- `/start` - Start the bot and see welcome message
- `/help` - Show detailed help and CSV format information
- `/status` - Check bot status and database connection
- `/logos` - Show available universities with logos
- `/stats` - Show processing statistics from the last import
- `/cancel` - Cancel any ongoing operation

## CSV File Format

Your CSV file should contain the following columns:

| Column | Description | Required |
|--------|-------------|----------|
| `name` | Smartject title | Yes |
| `mission` | Mission statement | No |
| `problematics` | Problem description | No |
| `scope` | Scope of the project | No |
| `audience` | Target audience (text) | No |
| `how it works` | How it works | No |
| `architecture` | Technical architecture | No |
| `innovation` | Innovation aspects | No |
| `use case` | Use cases | No |
| `industries` | Industries (comma-separated) | Yes* |
| `functions` | Business functions (comma-separated) | Yes* |
| `team` | Team members/universities (comma-separated) | No |
| `link` | Research paper link | No |
| `publish_date` | Publication date | No |
| `summarized` | Summary status | No |
| `url` | Source URL (NOT used for logos) | No |

*At least one tag from industries, audience types, and business functions is required.

### Special Values

- Records with `summarized` = "NO (not relevant)" will be skipped
- Existing smartjects (matched by title) will be skipped

### Example CSV

```csv
name;mission;problematics;scope;audience;how it works;architecture;innovation;use case;industries;functions;team;link;publish_date;summarized;url
"Smart Energy Monitor";"Reduce energy consumption";"High energy costs";"Residential buildings";"Homeowners";"IoT sensors track usage";"Cloud-based platform";"AI predictions";"Home automation";"Energy, IoT";"Operations, Analytics";"MIT, Stanford University";"https://example.com/paper";"2024-01-15";"YES";"https://example.com/logo.png"
```

## How It Works

1. **File Upload**: User sends a CSV file to the bot
2. **Validation**: Bot checks file format and size
3. **Processing**: For each record:
   - Check if marked as "not relevant" → Skip
   - Check if already exists in database → Skip
   - Parse and validate tags (industries, audience, functions)
   - Match university logos from team field (set to null if no match)
   - Insert into Supabase with all relations
4. **Teams Sync**: After processing all records:
   - Extract all unique team names from smartjects
   - Create missing teams in the teams table
   - Create smartject-team relationships
5. **Results**: Bot sends summary and detailed results

## University Logo Matching

The bot automatically matches university logos based on team members using:
- Direct name matching
- Case-insensitive matching
- Partial name matching
- Normalized name matching (removing common prefixes/suffixes)

**Note:** If no matching university logo is found, the `image_url` field is set to `null`. The `url` field from the CSV is NOT used as the image source.

Place your university logos CSV file in the `logos/` directory:
```csv
university;logo
Stanford University;https://example.com/stanford-logo.png
MIT;https://example.com/mit-logo.png
```

## Project Structure

```
smartjects-tg-bot/
├── bot/
│   ├── main.py              # Bot entry point
│   ├── handlers/            # Message and command handlers
│   │   ├── command_handler.py
│   │   └── file_handler.py
│   └── services/            # Business logic
│       ├── supabase_client.py
│       ├── csv_processor.py
│       └── logo_matcher.py
├── logos/                   # University logos CSV files
├── migrations/              # SQL migration files
│   └── create_team_functions.sql
├── requirements.txt         # Python dependencies
├── .env.example            # Environment variables template
└── README.md               # This file
```

## Troubleshooting

### Bot doesn't respond
- Check if BOT_TOKEN is correct in `.env`
- Ensure bot is running (`python bot/main.py`)
- Check if your user ID is in ALLOWED_USERS (if configured)

### Database connection errors
- Verify SUPABASE_URL and SUPABASE_KEY in `.env`
- Check if Supabase instance is running (for local setup)
- Ensure all required tables exist in Supabase

### CSV processing errors
- Verify CSV file format matches the requirements
- Check for encoding issues (UTF-8 is required)
- Ensure required columns are present
- Check that tag values match reference tables in database

### Teams not syncing
- Ensure teams and smartject_teams tables exist in database
- Run the SQL migration in `migrations/create_team_functions.sql`
- Check logs for any team synchronization errors

### Logo matching not working
- Verify logos CSV file exists in `logos/` directory
- Check file format (semicolon-separated with `university;logo` headers)
- Ensure university names in team field match logos file

### Rate limit errors ("Flood control exceeded")
This happens when the bot sends too many updates to Telegram too quickly.

**Solutions:**
- The bot now automatically handles rate limits and retries
- For large files (>100 records), the bot processes in batches
- Adjust these settings in `.env` if needed:
  ```env
  PROGRESS_UPDATE_INTERVAL=3.0  # Increase for less frequent updates
  BATCH_SIZE=30                 # Decrease for smaller batches
  BATCH_DELAY=0.2              # Increase for more delay between batches
  ```

**Best practices:**
- Split very large CSV files into smaller chunks (e.g., 500 records each)
- Process during off-peak hours for better performance
- If you consistently hit rate limits, consider increasing `PROGRESS_UPDATE_INTERVAL`

## Security Notes

- Store sensitive credentials in `.env` file (never commit this file)
- Use ALLOWED_USERS to restrict bot access if needed
- The bot has file size limits (10MB) to prevent abuse
- All database operations use parameterized queries

## Contributing

Feel free to submit issues and enhancement requests!

## License

This project is licensed under the MIT License.