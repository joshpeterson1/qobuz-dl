# Wrapper for Qo-DL Reborn. This is a sligthly modified version
# of qopy, originally written by Sorrow446. All credits to the
# original author.

import hashlib
import json
import logging
import os
import time
from datetime import datetime

import requests

from qobuz_dl.exceptions import (
    AuthenticationError,
    IneligibleError,
    InvalidAppIdError,
    InvalidAppSecretError,
    InvalidQuality,
)
from qobuz_dl.color import GREEN, YELLOW

RESET = "Reset your credentials with 'qobuz-dl -r'"

logger = logging.getLogger(__name__)


class Client:
    def __init__(self, email, pwd, app_id, secrets, api_delay=1.0, rate_limiter=None):
        logger.info(f"{YELLOW}Logging...")
        self.secrets = secrets
        self.id = str(app_id)
        self.api_delay = api_delay
        self.rate_limiter = rate_limiter
        self.email = email
        self.pwd = pwd
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:83.0) Gecko/20100101 Firefox/83.0",
                "X-App-Id": self.id,
                "Content-Type": "application/json;charset=UTF-8"

            }
        )
        self.base = "https://www.qobuz.com/api.json/0.2/"
        self.sec = None
        self.uat = None
        self.label = None
        
        # Set up token cache path
        if os.name == "nt":
            config_dir = os.environ.get("APPDATA")
        else:
            config_dir = os.path.join(os.environ["HOME"], ".config")
        self.token_cache_path = os.path.join(config_dir, "qobuz-dl", "token_cache.json")
        
        # Try to use cached token first, fall back to fresh auth
        if not self._try_cached_token():
            self.auth(email, pwd)
        
        self.cfg_setup()

    def api_call(self, epoint, **kwargs):
        # Apply rate limiting delay BEFORE the call to actually throttle
        if epoint != "user/login":
            if self.rate_limiter:
                current_api_delay, _ = self.rate_limiter.get_delays()
                if current_api_delay > 0:
                    time.sleep(current_api_delay)
            elif self.api_delay > 0:
                time.sleep(self.api_delay)

        params = self._build_params(epoint, **kwargs)
        r = self.session.get(self.base + epoint, params=params, timeout=30)

        if epoint == "user/login":
            if r.status_code == 401:
                raise AuthenticationError("Invalid credentials.\n" + RESET)
            elif r.status_code == 400:
                raise InvalidAppIdError("Invalid app id.\n" + RESET)
        elif (
            epoint in ["track/getFileUrl", "favorite/getUserFavorites"]
            and r.status_code == 400
        ):
            raise InvalidAppSecretError(f"Invalid app secret: {r.json()}.\n" + RESET)

        if r.status_code == 401 and epoint != "user/login":
            self._clear_token_cache()
            raise AuthenticationError(
                "Authentication token expired mid-session.\n" + RESET
            )

        r.raise_for_status()

        if epoint == "user/login":
            logger.info(f"{GREEN}Logged: OK")

        return r.json()

    def _build_params(self, epoint, **kwargs):
        if epoint == "user/login":
            return {
                "email": kwargs["email"],
                "password": kwargs["pwd"],
                "app_id": self.id,
            }
        elif epoint == "track/get":
            return {"track_id": kwargs["id"]}
        elif epoint == "album/get":
            return {"album_id": kwargs["id"]}
        elif epoint == "playlist/get":
            return {
                "extra": "tracks",
                "playlist_id": kwargs["id"],
                "limit": 500,
                "offset": kwargs["offset"],
            }
        elif epoint == "artist/get":
            return {
                "app_id": self.id,
                "artist_id": kwargs["id"],
                "limit": 500,
                "offset": kwargs["offset"],
                "extra": "albums",
            }
        elif epoint == "label/get":
            return {
                "label_id": kwargs["id"],
                "limit": 500,
                "offset": kwargs["offset"],
                "extra": "albums",
            }
        elif epoint == "favorite/getUserFavorites":
            unix = time.time()
            r_sig = "favoritegetUserFavorites" + str(unix) + kwargs["sec"]
            r_sig_hashed = hashlib.md5(r_sig.encode("utf-8")).hexdigest()
            return {
                "app_id": self.id,
                "user_auth_token": self.uat,
                "type": "albums",
                "request_ts": unix,
                "request_sig": r_sig_hashed,
            }
        elif epoint == "track/getFileUrl":
            unix = time.time()
            track_id = kwargs["id"]
            fmt_id = kwargs["fmt_id"]
            if int(fmt_id) not in (5, 6, 7, 27):
                raise InvalidQuality("Invalid quality id: choose between 5, 6, 7 or 27")
            r_sig = "trackgetFileUrlformat_id{}intentstreamtrack_id{}{}{}".format(
                fmt_id, track_id, unix, kwargs.get("sec", self.sec)
            )
            r_sig_hashed = hashlib.md5(r_sig.encode("utf-8")).hexdigest()
            return {
                "request_ts": unix,
                "request_sig": r_sig_hashed,
                "track_id": track_id,
                "format_id": fmt_id,
                "intent": "stream",
            }
        else:
            return kwargs

    def auth(self, email, pwd):
        logger.info(f"{YELLOW}Authenticating with fresh login...")
        usr_info = self.api_call("user/login", email=email, pwd=pwd)
        if not usr_info["user"]["credential"]["parameters"]:
            raise IneligibleError("Free accounts are not eligible to download tracks.")
        self.uat = usr_info["user_auth_token"]
        self.session.headers.update({"X-User-Auth-Token": self.uat})
        self.label = usr_info["user"]["credential"]["parameters"]["short_label"]
        logger.info(f"{GREEN}Membership: {self.label}")
        
        # Cache the token
        self._cache_token()

    def multi_meta(self, epoint, key, id, type):
        total = 1
        offset = 0
        while total > 0:
            if type in ["tracks", "albums"]:
                j = self.api_call(epoint, id=id, offset=offset, type=type)[type]
            else:
                j = self.api_call(epoint, id=id, offset=offset, type=type)
            if offset == 0:
                yield j
                total = j[key] - 500
            else:
                yield j
                total -= 500
            offset += 500

    def get_album_meta(self, id):
        return self.api_call("album/get", id=id)

    def get_track_meta(self, id):
        return self.api_call("track/get", id=id)

    def get_track_url(self, id, fmt_id):
        return self.api_call("track/getFileUrl", id=id, fmt_id=fmt_id)

    def get_artist_meta(self, id):
        return self.multi_meta("artist/get", "albums_count", id, None)

    def get_plist_meta(self, id):
        return self.multi_meta("playlist/get", "tracks_count", id, None)

    def get_label_meta(self, id):
        return self.multi_meta("label/get", "albums_count", id, None)

    def search_albums(self, query, limit):
        return self.api_call("album/search", query=query, limit=limit)

    def search_artists(self, query, limit):
        return self.api_call("artist/search", query=query, limit=limit)

    def search_playlists(self, query, limit):
        return self.api_call("playlist/search", query=query, limit=limit)

    def search_tracks(self, query, limit):
        return self.api_call("track/search", query=query, limit=limit)

    def get_favorite_albums(self, offset, limit):
        return self.api_call(
            "favorite/getUserFavorites", type="albums", offset=offset, limit=limit
        )

    def get_favorite_tracks(self, offset, limit):
        return self.api_call(
            "favorite/getUserFavorites", type="tracks", offset=offset, limit=limit
        )

    def get_favorite_artists(self, offset, limit):
        return self.api_call(
            "favorite/getUserFavorites", type="artists", offset=offset, limit=limit
        )

    def get_user_playlists(self, limit):
        return self.api_call("playlist/getUserPlaylists", limit=limit)

    def test_secret(self, sec):
        try:
            self.api_call("track/getFileUrl", id=5966783, fmt_id=5, sec=sec)
            return True
        except InvalidAppSecretError:
            return False

    def cfg_setup(self):
        for secret in self.secrets:
            # Falsy secrets
            if not secret:
                continue

            if self.test_secret(secret):
                self.sec = secret
                break

        if self.sec is None:
            raise InvalidAppSecretError("Can't find any valid app secret.\n" + RESET)

    def _try_cached_token(self):
        """Try to load and use a cached authentication token."""
        try:
            if not os.path.exists(self.token_cache_path):
                logger.info(f"{YELLOW}No cached token found")
                return False
                
            with open(self.token_cache_path, 'r') as f:
                cache_data = json.load(f)
            
            # Check if cache is for the same email
            if cache_data.get('user_email') != self.email:
                logger.info(f"{YELLOW}Cached token is for different user, ignoring")
                return False
                
            # Check if token has expired (7-day TTL)
            cached_at = cache_data.get('cached_at')
            if cached_at:
                try:
                    cache_time = datetime.fromisoformat(cached_at)
                    if (datetime.now() - cache_time).days > 7:
                        logger.info(f"{YELLOW}Cached token expired (older than 7 days)")
                        return False
                except (ValueError, TypeError):
                    pass

            cached_token = cache_data.get('user_auth_token')
            if not cached_token:
                logger.info(f"{YELLOW}Invalid cached token data")
                return False
                
            # Try to use the cached token
            self.uat = cached_token
            self.session.headers.update({"X-User-Auth-Token": self.uat})
            
            # Test the token with a simple API call
            try:
                # Use a lightweight call to test token validity
                test_response = self.session.get(f"{self.base}user/get", params={
                    "app_id": self.id
                }, timeout=10)
                if test_response.status_code == 401:
                    logger.info(f"{YELLOW}Cached token is expired/invalid")
                    return False
                    
                logger.info(f"{GREEN}Using cached authentication token")
                # We still need to get user info for the label
                # But we can't call user/login again, so we'll set a default
                self.label = cache_data.get('user_label', 'Unknown')
                logger.info(f"{GREEN}Membership: {self.label}")
                return True
                
            except Exception as e:
                logger.info(f"{YELLOW}Error testing cached token: {e}")
                return False
                
        except Exception as e:
            logger.info(f"{YELLOW}Error loading cached token: {e}")
            return False

    def _cache_token(self):
        """Save the current authentication token to cache."""
        try:
            os.makedirs(os.path.dirname(self.token_cache_path), exist_ok=True)
            
            cache_data = {
                'user_auth_token': self.uat,
                'user_email': self.email,
                'user_label': self.label,
                'cached_at': datetime.now().isoformat()
            }
            
            with open(self.token_cache_path, 'w') as f:
                json.dump(cache_data, f, indent=2)

            # Restrict file permissions on Unix
            if os.name != "nt":
                os.chmod(self.token_cache_path, 0o600)

            logger.info(f"{GREEN}Authentication token cached")
            
        except Exception as e:
            logger.warning(f"{YELLOW}Failed to cache token: {e}")

    def _clear_token_cache(self):
        """Remove the cached token file."""
        try:
            if os.path.exists(self.token_cache_path):
                os.remove(self.token_cache_path)
                logger.info(f"{YELLOW}Cleared cached token")
        except Exception as e:
            logger.warning(f"{YELLOW}Failed to clear token cache: {e}")
