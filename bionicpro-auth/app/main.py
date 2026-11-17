import secrets
import uuid
import json
import logging
from fastapi import FastAPI, Request, Response, HTTPException, Depends
from fastapi.responses import RedirectResponse, JSONResponse
import httpx
import hashlib
import base64
from urllib.parse import urlencode
from fastapi import HTTPException

import redis.asyncio as redis
import asyncpg
from datetime import timedelta

import os

logger = logging.getLogger("bionicpro-auth")

KEYCLOAK_INTERNAL_URL = os.getenv("KEYCLOAK_INTERNAL_URL", "http://keycloak:8082")
KEYCLOAK_EXTERNAL_URL = os.getenv("KEYCLOAK_EXTERNAL_URL", "http://localhost:8082")
REALM = os.getenv("KEYCLOAK_REALM", "reports-realm")
CLIENT_ID = os.getenv("KEYCLOAK_CLIENT_ID", "reports-frontend")
CLIENT_SECRET = os.getenv("KEYCLOAK_CLIENT_SECRET", None)
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
API_URL = os.getenv("API_URL", "http://backend:8000")
SESSION_TTL = 3600

PROFILE_DB_DSN = os.getenv(
    "PROFILE_DB_DSN",
    "postgresql://crm_user:crm_password@crm_db:5432/crm_db",
)

redis_client = redis.from_url(REDIS_URL, decode_responses=True)
db_pool: asyncpg.Pool | None = None


async def fetch_userinfo(access_token: str) -> dict:
    """Запрашивает у Keycloak профиль пользователя."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{KEYCLOAK_INTERNAL_URL}/realms/{REALM}/protocol/openid-connect/userinfo",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        if resp.status_code != 200:
            logger.warning("userinfo request failed: %s", resp.status_code)
            return {}
        return resp.json()


async def save_user_profile(userinfo: dict):
    """Сохраняет/обновляет профиль пользователя в БД."""
    if not db_pool or not userinfo.get("sub"):
        return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO user_profiles
                    (sub, username, email, first_name, last_name, identity_provider, raw_profile, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, NOW())
                ON CONFLICT (sub) DO UPDATE SET
                    username = EXCLUDED.username,
                    email = EXCLUDED.email,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    identity_provider = EXCLUDED.identity_provider,
                    raw_profile = EXCLUDED.raw_profile,
                    updated_at = NOW()
                """,
                userinfo.get("sub"),
                userinfo.get("preferred_username"),
                userinfo.get("email"),
                userinfo.get("given_name"),
                userinfo.get("family_name"),
                userinfo.get("identity_provider") or userinfo.get("iss"),
                json.dumps(userinfo),
            )
    except Exception as exc:
        logger.error("failed to save user profile: %s", exc)

async def save_session(session_id: str, data: dict):
    await redis_client.setex(
        f"session:{session_id}",
        timedelta(seconds=SESSION_TTL),
        json.dumps(data)
    )

async def get_session(session_id: str) -> dict:
    raw = await redis_client.get(f"session:{session_id}")
    if raw:
        return json.loads(raw)
    return None

async def delete_session(session_id: str):
    await redis_client.delete(f"session:{session_id}")

async def rotate_session(old_id: str, new_id: str):
    data = await get_session(old_id)
    if data:
        await save_session(new_id, data)
        await delete_session(old_id)

    code_verifier = secrets.token_urlsafe(96)[:128]
    code_challenge = base64.urlsafe_b64encode(
        hashlib.sha256(code_verifier.encode('ascii')).digest()
    ).rstrip(b'=').decode('ascii')
    return code_verifier, code_challenge

def generate_pkce_pair():
    code_verifier = secrets.token_urlsafe(96)[:128]
    code_challenge = base64.urlsafe_b64encode(
        hashlib.sha256(code_verifier.encode('ascii')).digest()
    ).rstrip(b'=').decode('ascii')
    return code_verifier, code_challenge

def get_authorization_url(redirect_uri: str, state: str):
    verifier, challenge = generate_pkce_pair()
    params = {
        "response_type": "code",
        "client_id": CLIENT_ID,
        "redirect_uri": redirect_uri,
        "scope": "openid profile email",
        "state": state,
        "code_challenge": challenge,
        "code_challenge_method": "S256"
    }
    url = f"{KEYCLOAK_EXTERNAL_URL}/realms/{REALM}/protocol/openid-connect/auth?" + urlencode(params)
    return url, verifier

async def exchange_code_for_tokens(code: str, redirect_uri: str, code_verifier: str):
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{KEYCLOAK_INTERNAL_URL}/realms/{REALM}/protocol/openid-connect/token",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "redirect_uri": redirect_uri,
                "code_verifier": code_verifier
            }
        )
        if resp.status_code != 200:
            raise HTTPException(status_code=400, detail="Token exchange failed")
        return resp.json()

async def refresh_access_token(refresh_token: str):
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{KEYCLOAK_INTERNAL_URL}/realms/{REALM}/protocol/openid-connect/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET
            }
        )
        if resp.status_code != 200:
            raise HTTPException(status_code=401, detail="Token refresh failed")
        return resp.json()

app = FastAPI(title="bionicpro-auth")


@app.on_event("startup")
async def init_db_pool():
    """Создаёт пул подключений к БД профилей."""
    global db_pool
    import asyncio
    for attempt in range(10):
        try:
            db_pool = await asyncpg.create_pool(
                dsn=PROFILE_DB_DSN, min_size=1, max_size=5
            )
            logger.info("profile DB pool created")
            return
        except Exception as exc:
            logger.warning("DB not ready (attempt %s): %s", attempt + 1, exc)
            await asyncio.sleep(3)
    logger.error("could not connect to profile DB; profiles will not be saved")


@app.on_event("shutdown")
async def close_db_pool():
    if db_pool:
        await db_pool.close()


@app.get("/auth/login")
async def login():
    state = secrets.token_urlsafe(32)
    redirect_uri = "http://localhost:5001/auth/callback"
    url, verifier = get_authorization_url(redirect_uri, state)
    await redis_client.setex(f"pkce:{state}", 300, verifier)
    response = RedirectResponse(url)
    return response

@app.get("/auth/callback")
async def callback(request: Request):
    code = request.query_params.get("code")
    state = request.query_params.get("state")
    if not code:
        raise HTTPException(status_code=400, detail="Missing code")
    verifier = await redis_client.get(f"pkce:{state}")
    if not verifier:
        raise HTTPException(status_code=400, detail="Invalid state")
    await redis_client.delete(f"pkce:{state}")
    redirect_uri = "http://localhost:5001/auth/callback"
    tokens = await exchange_code_for_tokens(code, redirect_uri, verifier)
    userinfo = await fetch_userinfo(tokens["access_token"])
    await save_user_profile(userinfo)
    session_id = str(uuid.uuid4())
    await save_session(session_id, {
        "access_token": tokens["access_token"],
        "refresh_token": tokens["refresh_token"],
        "id_token": tokens.get("id_token"),
        "user_info": userinfo
    })
    response = RedirectResponse(url="http://localhost:3000")
    response.set_cookie(
        key="bionicpro_session",
        value=session_id,
        httponly=True,
        secure=True,
        samesite="lax",
        max_age=3600
    )
    return response

@app.post("/auth/logout")
async def logout(request: Request, response: Response):
    session_id = request.cookies.get("bionicpro_session")
    if session_id:
        await delete_session(session_id)
    response.delete_cookie("bionicpro_session")
    return {"message": "Logged out"}


@app.get("/api/reports")
async def get_reports(request: Request):
    session_id = request.cookies.get("bionicpro_session")
    if not session_id:
        raise HTTPException(status_code=401)
    session_data = await get_session(session_id)
    if not session_data:
        raise HTTPException(status_code=401)
    access_token = session_data["access_token"]
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{API_URL}/reports",
            headers={"Authorization": f"Bearer {access_token}"}
        )
        if resp.status_code == 401:
            try:
                new_tokens = await refresh_access_token(session_data["refresh_token"])
                session_data["access_token"] = new_tokens["access_token"]
                session_data["refresh_token"] = new_tokens["refresh_token"]
                await save_session(session_id, session_data)
                resp = await client.get(
                    f"{API_URL}/reports",
                    headers={"Authorization": f"Bearer {new_tokens['access_token']}"}
                )
            except Exception:
                raise HTTPException(status_code=401)
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)

    new_session_id = str(uuid.uuid4())
    await rotate_session(session_id, new_session_id)
    response = Response(content=resp.content, media_type=resp.headers.get("content-type"))
    response.set_cookie(
        key="bionicpro_session",
        value=new_session_id,
        httponly=True,
        secure=True,
        samesite="lax",
        max_age=3600
    )
    return response