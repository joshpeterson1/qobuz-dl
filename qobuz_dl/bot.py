import asyncio
import configparser
import logging
import os
import re
import sys
import time

from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

from qobuz_dl.core import QobuzDL
from qobuz_dl.downloader import DEFAULT_FOLDER, DEFAULT_TRACK

logger = logging.getLogger(__name__)

if os.name == "nt":
    OS_CONFIG = os.environ.get("APPDATA")
else:
    OS_CONFIG = os.path.join(os.environ["HOME"], ".config")

CONFIG_PATH = os.path.join(OS_CONFIG, "qobuz-dl")
CONFIG_FILE = os.path.join(CONFIG_PATH, "config.ini")
QOBUZ_DB = os.path.join(CONFIG_PATH, "qobuz_dl.db")

URL_PATTERN = re.compile(r'(https?://\S*(?:qobuz\.com|spotify\.com|last\.fm)\S*)')
SYNC_FLAG = re.compile(r'!sync\b', re.IGNORECASE)


class QobuzTelegramBot:
    def __init__(self):
        config = configparser.ConfigParser()
        config.read(CONFIG_FILE)

        # Qobuz config (mirrors cli.py)
        defaults = config["DEFAULT"]
        email = defaults["email"]
        password = defaults["password"]
        app_id = defaults["app_id"]
        secrets = [s for s in defaults["secrets"].split(",") if s]

        # Telegram config
        if not config.has_section("telegram"):
            sys.exit(
                "No [telegram] section in config. Add it to: " + CONFIG_FILE
                + "\n\nRequired keys: bot_token, allowed_chat_ids"
            )
        tg = config["telegram"]
        self.bot_token = tg["bot_token"]
        self.allowed_chat_ids = set(
            int(x.strip()) for x in tg["allowed_chat_ids"].split(",")
        )
        self.qbt_host = tg.get("qbittorrent_host", "")
        self.qbt_user = tg.get("qbittorrent_username", "")
        self.qbt_pass = tg.get("qbittorrent_password", "")
        self.trackers = [
            t.strip() for t in tg.get("torrent_trackers", "").split(",") if t.strip()
        ] or None

        # Initialize QobuzDL
        self.qobuz = QobuzDL(
            directory=defaults.get("default_folder", "Qobuz Downloads"),
            quality=int(defaults.get("default_quality", "6")),
            embed_art=defaults.getboolean("embed_art", fallback=False),
            ignore_singles_eps=defaults.getboolean("albums_only", fallback=False),
            no_m3u_for_playlists=defaults.getboolean("no_m3u", fallback=False),
            quality_fallback=not defaults.getboolean("no_fallback", fallback=False),
            cover_og_quality=defaults.getboolean("og_cover", fallback=False),
            no_cover=defaults.getboolean("no_cover", fallback=False),
            downloads_db=QOBUZ_DB if not defaults.getboolean("no_database", fallback=False) else None,
            folder_format=defaults.get("folder_format", DEFAULT_FOLDER),
            track_format=defaults.get("track_format", DEFAULT_TRACK),
            smart_discography=defaults.getboolean("smart_discography", fallback=False),
            api_delay=float(defaults.get("api_delay", "1.0")),
            download_delay=float(defaults.get("download_delay", "0.3")),
            spotify_client_id=defaults.get("spotify_client_id", ""),
            spotify_client_secret=defaults.get("spotify_client_secret", ""),
        )
        self.qobuz.initialize_client(email, password, app_id, secrets)

        self.download_lock = asyncio.Lock()
        self.start_time = time.time()

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_chat.id not in self.allowed_chat_ids:
            return

        text = update.message.text or ""
        urls = URL_PATTERN.findall(text)
        if not urls:
            return

        do_sync = bool(SYNC_FLAG.search(text))

        for url in urls:
            async with self.download_lock:
                status_msg = await update.message.reply_text(f"⏳ Downloading: {url}")
                try:
                    loop = asyncio.get_event_loop()
                    path = await loop.run_in_executor(None, self._do_download, url)

                    if path and os.path.isdir(path):
                        folder_name = os.path.basename(path)
                        await status_msg.edit_text(f"✅ Downloaded: {folder_name}")

                        if do_sync:
                            await self._handle_sync(update, path, folder_name)
                    else:
                        await status_msg.edit_text(f"✅ Download completed")

                except Exception as e:
                    logger.exception(f"Download failed for {url}")
                    await status_msg.edit_text(f"❌ Error: {e}")

    def _do_download(self, url):
        if "last.fm" in url:
            return self.qobuz.download_lastfm_pl(url)
        elif "spotify.com" in url:
            return self.qobuz.download_spotify_pl(url)
        else:
            return self.qobuz.handle_url(url)

    async def _handle_sync(self, update, path, folder_name):
        try:
            from qobuz_dl.torrent import create_torrent, seed_via_qbittorrent

            loop = asyncio.get_event_loop()
            magnet, torrent_path = await loop.run_in_executor(
                None, create_torrent, path, self.trackers
            )

            if self.qbt_host:
                save_path = os.path.dirname(path)
                await loop.run_in_executor(
                    None, seed_via_qbittorrent,
                    torrent_path, save_path,
                    self.qbt_host, self.qbt_user, self.qbt_pass,
                )
                await update.message.reply_text(
                    f"🔗 Seeding: {folder_name}\n\n`{magnet}`",
                    parse_mode="Markdown",
                )
            else:
                await update.message.reply_text(
                    f"🔗 Torrent: {folder_name}\n\n`{magnet}`",
                    parse_mode="Markdown",
                )
        except Exception as e:
            logger.exception("Torrent creation failed")
            await update.message.reply_text(f"❌ Torrent error: {e}")

    async def handle_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_chat.id not in self.allowed_chat_ids:
            return
        await update.message.reply_text(
            "Send me a link to download:\n"
            "• Qobuz album/track/playlist/artist URLs\n"
            "• Spotify playlist URLs\n"
            "• Last.fm playlist URLs\n\n"
            "Add `!sync` to create a torrent + magnet link.\n\n"
            "Commands: /help /status"
        )

    async def handle_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_chat.id not in self.allowed_chat_ids:
            return
        uptime = int(time.time() - self.start_time)
        hours, remainder = divmod(uptime, 3600)
        minutes, seconds = divmod(remainder, 60)
        await update.message.reply_text(
            f"🟢 Bot is running\n"
            f"Uptime: {hours}h {minutes}m {seconds}s\n"
            f"Download dir: {self.qobuz.directory}"
        )

    def start(self):
        app = Application.builder().token(self.bot_token).build()
        app.add_handler(CommandHandler("help", self.handle_help))
        app.add_handler(CommandHandler("start", self.handle_help))
        app.add_handler(CommandHandler("status", self.handle_status))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
        logger.info("Bot started, listening for messages...")
        app.run_polling()


def run_bot():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    bot = QobuzTelegramBot()
    bot.start()


if __name__ == "__main__":
    run_bot()
