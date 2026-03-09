import logging
import os
import time
from typing import Tuple
from urllib3.exceptions import IncompleteRead

import requests
from pathvalidate import sanitize_filename, sanitize_filepath
from tqdm import tqdm

import qobuz_dl.metadata as metadata
from qobuz_dl.color import OFF, GREEN, RED, YELLOW, CYAN
from qobuz_dl.exceptions import NonStreamable

QL_DOWNGRADE = "FormatRestrictedByFormatAvailability"
# used in case of error
DEFAULT_FORMATS = {
    "MP3": [
        "{artist} - {album} ({year}) [MP3]",
        "{tracknumber}. {tracktitle}",
    ],
    "Unknown": [
        "{artist} - {album}",
        "{tracknumber}. {tracktitle}",
    ],
}

DEFAULT_FOLDER = "{artist} - {album} ({year}) [{bit_depth}B-{sampling_rate}kHz]"
DEFAULT_TRACK = "{tracknumber}. {tracktitle}"

logger = logging.getLogger(__name__)


class RateLimiter:
    """Manages dynamic rate limiting with escalation on failures."""
    
    def __init__(self, initial_api_delay=1.0, initial_download_delay=0.3):
        self.initial_api_delay = initial_api_delay
        self.initial_download_delay = initial_download_delay
        self.current_api_delay = initial_api_delay
        self.current_download_delay = initial_download_delay
        self.escalation_count = 0
        self.successful_downloads = 0
        
    def escalate(self):
        """Double the rate limits due to failures."""
        self.escalation_count += 1
        self.successful_downloads = 0  # Reset success counter on failure
        self.current_api_delay *= 2
        self.current_download_delay *= 2

        # Cap at reasonable maximums
        self.current_api_delay = min(self.current_api_delay, 30.0)
        self.current_download_delay = min(self.current_download_delay, 10.0)

        logger.warning(
            f"{YELLOW}Rate limits escalated (level {self.escalation_count}): "
            f"API={self.current_api_delay:.1f}s, Download={self.current_download_delay:.1f}s"
        )

    def record_success(self):
        """Record a successful download and step down rate limits gradually."""
        self.successful_downloads += 1

        if self.successful_downloads >= 10 and self.escalation_count > 0:
            # Step down one level instead of full reset
            self.current_api_delay = max(self.current_api_delay / 2, self.initial_api_delay)
            self.current_download_delay = max(self.current_download_delay / 2, self.initial_download_delay)
            self.escalation_count = max(self.escalation_count - 1, 0)
            self.successful_downloads = 0
            if self.escalation_count > 0:
                logger.info(
                    f"{GREEN}Rate limits reduced (level {self.escalation_count}): "
                    f"API={self.current_api_delay:.1f}s, Download={self.current_download_delay:.1f}s"
                )
            else:
                logger.info(f"{GREEN}Rate limits reset to normal after successful downloads")
    
    def get_delays(self):
        """Get current delay values."""
        return self.current_api_delay, self.current_download_delay


def _log_failed_track(track_desc, error_msg):
    """Log failed track downloads to a file."""
    try:
        # Use same config directory as the main app
        if os.name == "nt":
            config_dir = os.environ.get("APPDATA")
        else:
            config_dir = os.path.join(os.environ["HOME"], ".config")
        
        log_dir = os.path.join(config_dir, "qobuz-dl")
        os.makedirs(log_dir, exist_ok=True)
        
        failed_log_path = os.path.join(log_dir, "failed_downloads.log")
        
        with open(failed_log_path, "a", encoding="utf-8") as f:
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"[{timestamp}] FAILED: {track_desc}\n")
            f.write(f"    Error: {error_msg}\n\n")
            
        logger.info(f"{YELLOW}Failed download logged to: {failed_log_path}")
        
    except Exception as e:
        logger.warning(f"{YELLOW}Could not log failed download: {e}")


class Download:
    def __init__(
        self,
        client,
        item_id: str,
        path: str,
        quality: int,
        embed_art: bool = False,
        albums_only: bool = False,
        downgrade_quality: bool = False,
        cover_og_quality: bool = False,
        no_cover: bool = False,
        folder_format=None,
        track_format=None,
        rate_limiter=None,
    ):
        self.client = client
        self.item_id = item_id
        self.path = path
        self.quality = quality
        self.albums_only = albums_only
        self.embed_art = embed_art
        self.downgrade_quality = downgrade_quality
        self.cover_og_quality = cover_og_quality
        self.no_cover = no_cover
        self.folder_format = folder_format or DEFAULT_FOLDER
        self.track_format = track_format or DEFAULT_TRACK
        self.rate_limiter = rate_limiter

    def download_id_by_type(self, track=True):
        if not track:
            return self.download_release()
        else:
            return self.download_track()

    def download_release(self):
        count = 0
        failed = 0
        meta = self.client.get_album_meta(self.item_id)

        if not meta.get("streamable"):
            raise NonStreamable("This release is not streamable")

        if self.albums_only and (
            meta.get("release_type") != "album"
            or meta.get("artist").get("name") == "Various Artists"
        ):
            logger.info(f'{OFF}Ignoring Single/EP/VA: {meta.get("title", "n/a")}')
            return True, None

        album_title = _get_title(meta)

        format_info = self._get_format(meta)
        file_format, quality_met, bit_depth, sampling_rate = format_info

        if not self.downgrade_quality and not quality_met:
            logger.info(
                f"{OFF}Skipping {album_title} as it doesn't meet quality requirement"
            )
            return True, None

        logger.info(
            f"\n{YELLOW}Downloading: {album_title}\nQuality: {file_format}"
            f" ({bit_depth}/{sampling_rate})\n"
        )
        album_attr = self._get_album_attr(
            meta, album_title, file_format, bit_depth, sampling_rate
        )
        folder_format, track_format = _clean_format_str(
            self.folder_format, self.track_format, file_format
        )
        sanitized_title = sanitize_filepath(folder_format.format(**album_attr))
        dirn = os.path.join(self.path, sanitized_title)
        os.makedirs(dirn, exist_ok=True)

        if self.no_cover:
            logger.info(f"{OFF}Skipping cover")
        else:
            try:
                _get_extra(meta["image"]["large"], dirn, og_quality=self.cover_og_quality)
            except Exception as e:
                logger.error(f"{RED}Failed to download cover: {e}")

        if "goodies" in meta:
            try:
                _get_extra(meta["goodies"][0]["url"], dirn, "booklet.pdf")
            except Exception:
                pass
        media_numbers = [track["media_number"] for track in meta["tracks"]["items"]]
        is_multiple = len(set(media_numbers)) > 1
        for i in meta["tracks"]["items"]:
            parse = self.client.get_track_url(i["id"], fmt_id=self.quality)
            if "sample" not in parse and parse["sampling_rate"]:
                is_mp3 = int(self.quality) == 5
                success = self._download_and_tag(
                    dirn,
                    count,
                    parse,
                    i,
                    meta,
                    False,
                    is_mp3,
                    i["media_number"] if is_multiple else None,
                )
                if not success:
                    failed += 1
            else:
                logger.info(f"{OFF}Demo. Skipping")
            count = count + 1

        total = len(meta["tracks"]["items"])
        downloaded = total - failed
        if failed:
            logger.info(
                f"{YELLOW}Completed with errors: {downloaded}/{total} tracks"
                f" downloaded, {failed} failed"
            )
        else:
            logger.info(f"{GREEN}Completed: {downloaded}/{total} tracks downloaded")
        return failed == 0, dirn

    def download_track(self):
        parse = self.client.get_track_url(self.item_id, self.quality)

        if "sample" not in parse and parse["sampling_rate"]:
            meta = self.client.get_track_meta(self.item_id)
            track_title = _get_title(meta)
            artist = _safe_get(meta, "performer", "name")
            logger.info(f"\n{YELLOW}Downloading: {artist} - {track_title}")
            format_info = self._get_format(meta, is_track_id=True, track_url_dict=parse)
            file_format, quality_met, bit_depth, sampling_rate = format_info

            folder_format, track_format = _clean_format_str(
                self.folder_format, self.track_format, str(bit_depth)
            )

            if not self.downgrade_quality and not quality_met:
                logger.info(
                    f"{OFF}Skipping {track_title} as it doesn't "
                    "meet quality requirement"
                )
                return True, None
            track_attr = self._get_track_attr(
                meta, track_title, bit_depth, sampling_rate
            )
            sanitized_title = sanitize_filepath(folder_format.format(**track_attr))

            dirn = os.path.join(self.path, sanitized_title)
            os.makedirs(dirn, exist_ok=True)
            if self.no_cover:
                logger.info(f"{OFF}Skipping cover")
            else:
                try:
                    _get_extra(
                        meta["album"]["image"]["large"],
                        dirn,
                        og_quality=self.cover_og_quality,
                    )
                except Exception as e:
                    logger.error(f"{RED}Failed to download cover: {e}")
            is_mp3 = int(self.quality) == 5
            success = self._download_and_tag(
                dirn,
                1,
                parse,
                meta,
                meta,
                True,
                is_mp3,
                False,
            )
            logger.info(f"{GREEN}Completed")
            return success, dirn
        else:
            logger.info(f"{OFF}Demo. Skipping")
            return True, None

    def _download_and_tag(
        self,
        root_dir,
        tmp_count,
        track_url_dict,
        track_metadata,
        album_or_track_metadata,
        is_track,
        is_mp3,
        multiple=None,
    ):
        extension = ".mp3" if is_mp3 else ".flac"

        try:
            url = track_url_dict["url"]
        except KeyError:
            logger.info(f"{OFF}Track not available for download")
            _log_failed_track(
                track_metadata.get("title", "unknown"), "Track URL not available"
            )
            return False

        if multiple:
            root_dir = os.path.join(root_dir, f"Disc {multiple}")
            os.makedirs(root_dir, exist_ok=True)

        filename = os.path.join(root_dir, f".{tmp_count:02}.tmp")

        # Determine the filename
        track_title = track_metadata.get("title")
        artist = _safe_get(track_metadata, "performer", "name")
        filename_attr = self._get_filename_attr(artist, track_metadata, track_title)

        # track_format is a format string
        # e.g. '{tracknumber}. {artist} - {tracktitle}'
        formatted_path = sanitize_filename(self.track_format.format(**filename_attr))
        # Truncate filename (not full path) to stay within OS path limits
        max_filename_len = 250 - len(root_dir) - len(extension) - 1
        if max_filename_len < 10:
            max_filename_len = 10
        if len(formatted_path) > max_filename_len:
            logger.warning(f"{YELLOW}Filename truncated: {formatted_path}")
            formatted_path = formatted_path[:max_filename_len].rstrip(". ")
        full_path = os.path.join(root_dir, formatted_path)
        final_file = full_path + extension

        if os.path.isfile(final_file):
            logger.info(f"{OFF}{track_title} was already downloaded")
            return True

        try:
            tqdm_download(url, filename, track_title, rate_limiter=self.rate_limiter)

            # Record successful download
            if self.rate_limiter:
                self.rate_limiter.record_success()

            tag_function = metadata.tag_mp3 if is_mp3 else metadata.tag_flac
            try:
                tag_function(
                    filename,
                    root_dir,
                    final_file,
                    track_metadata,
                    album_or_track_metadata,
                    is_track,
                    self.embed_art,
                )
            except Exception as e:
                logger.error(f"{RED}Error tagging the file: {e}", exc_info=True)
                # Save untagged file rather than losing the download
                try:
                    if os.path.isfile(filename):
                        os.rename(filename, final_file)
                        logger.warning(f"{YELLOW}Saved untagged file: {final_file}")
                except OSError:
                    pass
            return True

        except Exception as e:
            logger.error(f"{RED}Failed to download {track_title}: {e}")
            _log_failed_track(track_title, str(e))
            return False

    @staticmethod
    def _get_filename_attr(artist, track_metadata, track_title):
        return {
            "artist": artist,
            "albumartist": _safe_get(
                track_metadata, "album", "artist", "name", default=artist
            ),
            "bit_depth": track_metadata["maximum_bit_depth"],
            "sampling_rate": track_metadata["maximum_sampling_rate"],
            "tracktitle": track_title,
            "version": track_metadata.get("version"),
            "tracknumber": f"{track_metadata['track_number']:02}",
        }

    @staticmethod
    def _get_track_attr(meta, track_title, bit_depth, sampling_rate):
        return {
            "album": sanitize_filename(meta["album"]["title"]),
            "artist": sanitize_filename(meta["album"]["artist"]["name"]),
            "tracktitle": track_title,
            "year": meta["album"]["release_date_original"].split("-")[0],
            "bit_depth": bit_depth,
            "sampling_rate": sampling_rate,
        }

    @staticmethod
    def _get_album_attr(meta, album_title, file_format, bit_depth, sampling_rate):
        return {
            "artist": sanitize_filename(meta["artist"]["name"]),
            "album": sanitize_filename(album_title),
            "year": meta["release_date_original"].split("-")[0],
            "format": file_format,
            "bit_depth": bit_depth,
            "sampling_rate": sampling_rate,
        }

    def _get_format(self, item_dict, is_track_id=False, track_url_dict=None):
        quality_met = True
        if int(self.quality) == 5:
            return ("MP3", quality_met, None, None)
        track_dict = item_dict
        if not is_track_id:
            track_dict = item_dict["tracks"]["items"][0]

        try:
            new_track_dict = (
                self.client.get_track_url(track_dict["id"], fmt_id=self.quality)
                if not track_url_dict
                else track_url_dict
            )
            restrictions = new_track_dict.get("restrictions")
            if isinstance(restrictions, list):
                if any(
                    restriction.get("code") == QL_DOWNGRADE
                    for restriction in restrictions
                ):
                    quality_met = False

            return (
                "FLAC",
                quality_met,
                new_track_dict["bit_depth"],
                new_track_dict["sampling_rate"],
            )
        except (KeyError, requests.exceptions.HTTPError):
            return ("Unknown", quality_met, None, None)


def tqdm_download(url, fname, desc, max_retries=3, rate_limiter=None):
    """Download a file with retry logic for connection issues."""
    if rate_limiter:
        _, download_delay = rate_limiter.get_delays()
        if download_delay > 0:
            time.sleep(download_delay)

    for attempt in range(max_retries):
        try:
            r = requests.get(url, allow_redirects=True, stream=True, timeout=30)
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0))
            download_size = 0
            
            with open(fname, "wb") as file, tqdm(
                total=total,
                unit="iB",
                unit_scale=True,
                unit_divisor=1024,
                desc=desc,
                bar_format=CYAN + "{n_fmt}/{total_fmt} /// {desc}",
            ) as bar:
                for data in r.iter_content(chunk_size=1024):
                    size = file.write(data)
                    bar.update(size)
                    download_size += size

            if total > 0 and total != download_size:
                raise ConnectionError(f"File download was interrupted for {fname}")
            
            return  # Success!
            
        except (IncompleteRead, ConnectionError, requests.exceptions.RequestException) as e:
            if attempt < max_retries - 1:  # Not the last attempt
                logger.warning(f"{YELLOW}Download failed (attempt {attempt + 1}/{max_retries}): {e}")
                logger.info(f"{YELLOW}Waiting 5 seconds before retry...")
                time.sleep(5)
                
                # Remove partial file
                try:
                    if os.path.exists(fname):
                        os.remove(fname)
                except OSError:
                    pass
            else:
                # Final failure - log it and escalate rate limits
                logger.error(f"{RED}Download failed after {max_retries} attempts: {e}")
                _log_failed_track(desc, str(e))
                
                if rate_limiter:
                    rate_limiter.escalate()
                
                raise e



def _get_title(item_dict):
    album_title = item_dict["title"]
    version = item_dict.get("version")
    if version:
        album_title = (
            f"{album_title} ({version})"
            if version.lower() not in album_title.lower()
            else album_title
        )
    return album_title


def _get_extra(item, dirn, extra="cover.jpg", og_quality=False):
    extra_file = os.path.join(dirn, extra)
    if os.path.isfile(extra_file):
        logger.info(f"{OFF}{extra} was already downloaded")
        return
    tqdm_download(
        item.replace("_600.", "_org.") if og_quality else item,
        extra_file,
        extra,
    )


def _clean_format_str(folder: str, track: str, file_format: str) -> Tuple[str, str]:
    """Cleans up the format strings, avoids errors
    with MP3 files.
    """
    final = []
    for i, fs in enumerate((folder, track)):
        if fs.endswith(".mp3"):
            fs = fs[:-4]
        elif fs.endswith(".flac"):
            fs = fs[:-5]
        fs = fs.strip()

        # default to pre-chosen string if format is invalid
        if file_format in ("MP3", "Unknown") and (
            "bit_depth" in fs or "sampling_rate" in fs
        ):
            default = DEFAULT_FORMATS[file_format][i]
            logger.error(
                f"{RED}invalid format string for format {file_format}"
                f". defaulting to {default}"
            )
            fs = default
        final.append(fs)

    return tuple(final)


def _safe_get(d: dict, *keys, default=None):
    """A replacement for chained `get()` statements on dicts:
    >>> d = {'foo': {'bar': 'baz'}}
    >>> _safe_get(d, 'baz')
    None
    >>> _safe_get(d, 'foo', 'bar')
    'baz'
    """
    curr = d
    res = default
    for key in keys:
        res = curr.get(key, default)
        if res == default or not hasattr(res, "__getitem__"):
            return res
        else:
            curr = res
    return res
