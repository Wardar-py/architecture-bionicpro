import secrets
import uuid
import json
from fastapi import FastAPI, Request, Response, HTTPException, Depends
from fastapi.responses import RedirectResponse, JSONResponse
import httpx
import hashlib
import base64
from urllib.parse import urlencode
from fastapi import HTTPException

import redis.asyncio as redis
from datetime import timedelta

import os

KEYCLOAK_INTERNAL_URL = os.getenv("KEYCLOAK_INTERNAL_URL", "http://keycloak:8082")  # для сервер-серверных запросов
KEYCLOAK_EXTERNAL_URL = os.getenv("KEYCLOAK_EXTERNAL_URL", "http://localhost:8082")  # для редиректов пользователя
REALM = os.getenv("KEYCLOAK_REALM", "reports-realm")
CLIENT_ID = os.getenv("KEYCLOAK_CLIENT_ID", "reports-frontend")
CLIENT_SECRET = os.getenv("KEYCLOAK_CLIENT_SECRET", None)
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
API_URL = os.getenv("API_URL", "http://backend:8000")
SESSION_TTL = 3600

redis_client = redis.from_url(REDIS_URL, decode_responses=True)

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
    # перепривязываем данные к новому ID, удаляем старый
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
    # ВАЖНО: используем внешний URL, чтобы браузер мог перейти
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


@app.get("/auth/login")
async def login():
    state = secrets.token_urlsafe(32)
    # в реальном приложении state нужно сохранять и проверять
    redirect_uri = "http://localhost:5001/auth/callback"
    url, verifier = get_authorization_url(redirect_uri, state)
    # временно сохраняем verifier, например, в Redis с ключом state
    # для простоты можно передать в куки (небезопасно), используем Redis
    await redis_client.setex(f"pkce:{state}", 300, verifier)
    response = RedirectResponse(url)
    return response

@app.get("/auth/callback")
async def callback(request: Request):
    code = request.query_params.get("code")
    state = request.query_params.get("state")
    if not code:
        raise HTTPException(status_code=400, detail="Missing code")
    # получаем code_verifier
    verifier = await redis_client.get(f"pkce:{state}")
    if not verifier:
        raise HTTPException(status_code=400, detail="Invalid state")
    await redis_client.delete(f"pkce:{state}")
    redirect_uri = "http://localhost:5001/auth/callback"
    tokens = await exchange_code_for_tokens(code, redirect_uri, verifier)
    # создаём сессию
    session_id = str(uuid.uuid4())
    await save_session(session_id, {
        "access_token": tokens["access_token"],
        "refresh_token": tokens["refresh_token"],
        "id_token": tokens.get("id_token"),
        "user_info": tokens.get("user_info", {})
    })
    response = RedirectResponse(url="/")  # или на фронтенд
    response.set_cookie(
        key="bionicpro_session",
        value=session_id,
        httponly=True,
        secure=True,  # для локальной разработки можно временно False
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

# Прокси для получения отчёта (защищённый ресурс)
@app.get("/api/reports")
async def get_reports(request: Request):
    session_id = request.cookies.get("bionicpro_session")
    if not session_id:
        raise HTTPException(status_code=401)
    session_data = await get_session(session_id)
    if not session_data:
        raise HTTPException(status_code=401)
    access_token = session_data["access_token"]
    # попытка выполнить запрос с текущим access_token
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{API_URL}/reports",
            headers={"Authorization": f"Bearer {access_token}"}
        )
        if resp.status_code == 401:  # токен истёк
            # обновляем
            try:
                new_tokens = await refresh_access_token(session_data["refresh_token"])
                session_data["access_token"] = new_tokens["access_token"]
                session_data["refresh_token"] = new_tokens["refresh_token"]
                await save_session(session_id, session_data)
                # повторяем запрос
                resp = await client.get(
                    f"{API_URL}/reports",
                    headers={"Authorization": f"Bearer {new_tokens['access_token']}"}
                )
            except Exception:
                raise HTTPException(status_code=401)
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
    # ротация сессии
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