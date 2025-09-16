import os
import time
from typing import Optional, Dict, Any
import asyncio

import dropbox
from dropbox.oauth import DropboxOAuth2FlowNoRedirect, DropboxOAuth2Flow
from json import dumps as json_dumps, loads as json_loads
from auth.cache import get_async_redis, REDIS_TTL_SECONDS, auth_key


DROPBOX_APP_KEY = os.environ.get("DROPBOX_APP_KEY")
DROPBOX_APP_SECRET = os.environ.get("DROPBOX_APP_SECRET")
_redis = get_async_redis()


def _dropbox_key(user_id: str) -> str:
    return auth_key("dropbox", user_id)


async def cache_dropbox_token(user_id: str, token: Dict[str, Any], ttl: int = REDIS_TTL_SECONDS) -> None:
    if "expires_at" not in token:
        token["expires_at"] = int(time.time()) + token.get("expires_in", REDIS_TTL_SECONDS)
    await _redis.set(_dropbox_key(user_id), json_dumps(token), ex=ttl)


async def get_dropbox_token(user_id: str) -> Optional[Dict[str, Any]]:
    raw = await _redis.get(_dropbox_key(user_id))
    return json_loads(raw) if raw else None


async def delete_dropbox_token(user_id: str) -> None:
    await _redis.delete(_dropbox_key(user_id))


def get_dropbox_client(access_token: Optional[str] = None, user_id: Optional[str] = None) -> dropbox.Dropbox:
    """Return a Dropbox client.

    Priority:
    - If a refresh token exists in Redis (for DEFAULT_USER_ID), use it.
    - Else if `DROPBOX_REFRESH_TOKEN` in env: use refresh-token auth (auto-refresh).
    - Else if `DROPBOX_ACCESS_TOKEN` in env or provided param: use access token.
    - For async usage inside the web app, prefer DropboxAuthManager.get_cached_client.
    """
    # Always use sync bridging for worker/ETL contexts
    user_id = user_id or os.environ.get("DEFAULT_USER_ID", "user")
    try:
        cached = asyncio.run(get_dropbox_token(user_id))
        if cached and cached.get("refresh_token"):
            return dropbox.Dropbox(
                oauth2_refresh_token=cached["refresh_token"],
                app_key=DROPBOX_APP_KEY,
                app_secret=DROPBOX_APP_SECRET,
            )
    except Exception:
        # Fall through to env-based methods on any cache failure
        pass

    refresh_token = os.environ.get("DROPBOX_REFRESH_TOKEN")
    app_key = DROPBOX_APP_KEY
    app_secret = DROPBOX_APP_SECRET

    if refresh_token and app_key and app_secret:
        # Opportunistically persist env-provided refresh token for this user
        try:
            payload = {"refresh_token": refresh_token, "expires_in": 4 * 60 * 60}
            asyncio.run(cache_dropbox_token(user_id, payload))
        except Exception:
            pass
        return dropbox.Dropbox(
            oauth2_refresh_token=refresh_token,
            app_key=app_key,
            app_secret=app_secret,
        )

    raise RuntimeError(
        "No Dropbox credentials found."
    )


class DropboxAuthManager:
    """Manage Dropbox OAuth (no-redirect) and token refresh via SDK.

    Use `get_authorize_url()` to obtain a URL, ask the user to paste the code,
    then call `finish_no_redirect(user_id, code)` to persist tokens in Redis.
    Later, call `get_cached_client(user_id)` to obtain a client that auto-refreshes.
    """

    def __init__(self, app_key: Optional[str] = None, app_secret: Optional[str] = None):
        self.app_key = app_key or DROPBOX_APP_KEY
        self.app_secret = app_secret or DROPBOX_APP_SECRET
        if not self.app_key or not self.app_secret:
            raise RuntimeError("DROPBOX_APP_KEY and DROPBOX_APP_SECRET must be set in the environment")

    def _flow(self) -> DropboxOAuth2FlowNoRedirect:
        # Request offline access to receive a refresh token
        return DropboxOAuth2FlowNoRedirect(self.app_key, self.app_secret, token_access_type="offline")

    def get_authorize_url(self) -> str:
        return self._flow().start()

    async def finish_no_redirect(self, user_id: str, code: str) -> Dict[str, Any]:
        flow = self._flow()
        res = flow.finish(code)
        token: Dict[str, Any] = {
            "access_token": getattr(res, "access_token", None),
            "refresh_token": getattr(res, "refresh_token", None),
            "expires_in": getattr(res, "expires_in", 4 * 60 * 60),  # default 4h if absent
            "scope": getattr(res, "scope", None),
            "account_id": getattr(res, "account_id", None),
            "uid": getattr(res, "user_id", None) or getattr(res, "uid", None),
        }
        await cache_dropbox_token(user_id, token)
        return token

    async def get_cached_client(self, user_id: str) -> dropbox.Dropbox:
        token = await get_dropbox_token(user_id)
        # Prefer refresh-token based client which auto-refreshes
        if token and token.get("refresh_token"):
            return dropbox.Dropbox(
                oauth2_refresh_token=token["refresh_token"],
                app_key=self.app_key,
                app_secret=self.app_secret,
            )
        # Fallback to access token if available
        if token and token.get("access_token"):
            return dropbox.Dropbox(token["access_token"])
        # Finally, use env-based fallback
        return get_dropbox_client()

    # -------- Redirect-based OAuth2 (smoother UX) --------
    def _redirect_flow(self, redirect_uri: str, session: dict) -> DropboxOAuth2Flow:
        # DropboxOAuth2Flow uses session to store CSRF token; we provide a dict
        return DropboxOAuth2Flow(
            consumer_key=self.app_key,
            consumer_secret=self.app_secret,
            redirect_uri=redirect_uri,
            session=session,
            csrf_token_session_key="dropbox-auth-csrf-token",
            token_access_type="offline",
        )

    async def get_authorize_url_redirect(self, user_id: str, redirect_uri: str) -> str:
        # Create a fresh session dict and persist it for callback
        sess: dict = {}
        flow = self._redirect_flow(redirect_uri, sess)
        url = flow.start()
        await _redis.set(auth_key("dropbox", f"csrf:{user_id}"), json_dumps(sess), ex=600)
        return url

    async def finish_redirect(self, user_id: str, code: str, state: str, redirect_uri: str) -> Dict[str, Any]:
        raw = await _redis.get(auth_key("dropbox", f"csrf:{user_id}"))
        sess = json_loads(raw) if raw else {}
        flow = self._redirect_flow(redirect_uri, sess)
        res = flow.finish({"code": code, "state": state})
        token: Dict[str, Any] = {
            "access_token": getattr(res, "access_token", None),
            "refresh_token": getattr(res, "refresh_token", None),
            "expires_in": getattr(res, "expires_in", 4 * 60 * 60),
            "scope": getattr(res, "scope", None),
            "account_id": getattr(res, "account_id", None),
            "uid": getattr(res, "user_id", None) or getattr(res, "uid", None),
        }
        await cache_dropbox_token(user_id, token)
        # Clean up CSRF session
        await _redis.delete(auth_key("dropbox", f"csrf:{user_id}"))
        return token
