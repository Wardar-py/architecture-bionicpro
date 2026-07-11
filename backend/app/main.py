import os
import io
import csv
import jwt
from jwt import PyJWKClient
import datetime
from datetime import timedelta
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse

from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import boto3
from botocore.client import Config
from clickhouse_driver import Client as CHClient


app = FastAPI()


KEYCLOAK_WELL_KNOWN = os.getenv("KEYCLOAK_WELL_KNOWN_URL", "http://keycloak:8082/realms/reports-realm/.well-known/openid-configuration")
JWKS_CLIENT = PyJWKClient(KEYCLOAK_WELL_KNOWN.replace(".well-known/openid-configuration", "/protocol/openid-connect/certs"))

security = HTTPBearer()

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    token = credentials.credentials
    try:
        signing_key = JWKS_CLIENT.get_signing_key_from_jwt(token)
        payload = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            audience="reports-api",
            options={"verify_exp": True}
        )
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")


# Конфигурация MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_user")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_password")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "bionicpro-reports")
MINIO_SECURE = False  # в локальной разработке

s3_client = boto3.client(
    's3',
    endpoint_url=f"http://{MINIO_ENDPOINT}",
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

# Убедимся, что бакет существует
try:
    s3_client.head_bucket(Bucket=MINIO_BUCKET)
except Exception:
    s3_client.create_bucket(Bucket=MINIO_BUCKET)

def get_ch_client():
    return CHClient(host='olap_db', user='demo', password='demo', database='default')

# Витрина отчётности. По умолчанию — витрина на базе CDC + MaterializedView (задание 4).
REPORT_TABLE = os.getenv("REPORT_TABLE", "report_customer_telemetry_cdc")

def generate_report_data(user_id: int, from_date: str, to_date: str) -> str:
    """Генерирует CSV-отчёт и возвращает его содержимое как строку."""
    ch = get_ch_client()
    query = f"""
    SELECT date, user_id, full_name, email, prosthesis_model,
           total_actions, avg_response_time_ms, max_response_time_ms,
           min_response_time_ms, usage_duration_sec
    FROM {REPORT_TABLE}
    WHERE user_id = %(user_id)s AND date BETWEEN %(from_date)s AND %(to_date)s
    ORDER BY date
    """
    rows = ch.execute(query, {
        'user_id': user_id,
        'from_date': from_date,
        'to_date': to_date
    })
    if not rows:
        return None
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["date", "user_id", "full_name", "email", "prosthesis_model",
                     "total_actions", "avg_response_time_ms", "max_response_time_ms",
                     "min_response_time_ms", "usage_duration_sec"])
    for row in rows:
        writer.writerow(row)
    return output.getvalue()

@app.get("/reports")
async def get_report(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    user: dict = Depends(get_current_user)
):
    # user_id приходит отдельным числовым claim из Keycloak (маппер user_id),
    # т.к. sub — это UUID и не совпадает с идентификатором в CRM/OLAP
    raw_user_id = user.get("user_id")
    if raw_user_id is None:
        raise HTTPException(status_code=403, detail="Token has no user_id claim")
    try:
        user_id = int(raw_user_id)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid user_id claim")

    # Установка периода по умолчанию (последние 30 дней)
    if not from_date and not to_date:
        to_date = (datetime.datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
        from_date = (datetime.datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")
    else:
        try:
            datetime.datetime.strptime(from_date, "%Y-%m-%d")
            datetime.datetime.strptime(to_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format")

    # Ключ в S3
    s3_key = f"reports/{user_id}/{from_date}_{to_date}.csv"

    # Проверяем наличие в MinIO
    try:
        s3_client.head_object(Bucket=MINIO_BUCKET, Key=s3_key)
        report_exists = True
    except Exception:
        report_exists = False

    if not report_exists:
        # Генерируем отчёт
        csv_content = generate_report_data(user_id, from_date, to_date)
        if not csv_content:
            raise HTTPException(status_code=404, detail="No data for the given period")
        # Загружаем в MinIO
        s3_client.put_object(
            Bucket=MINIO_BUCKET,
            Key=s3_key,
            Body=csv_content.encode('utf-8'),
            ContentType='text/csv'
        )

    # Ссылка на CDN (Nginx), который проксирует MinIO с кешированием.
    # Путь включает имя бакета: /<bucket>/<key>.
    cdn_base_url = os.getenv("CDN_BASE_URL", "http://localhost:8081")
    file_url = f"{cdn_base_url}/{MINIO_BUCKET}/{s3_key}"

    # Возвращаем ссылку для скачивания (редирект на CDN)
    return RedirectResponse(url=file_url)