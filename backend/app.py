import os
from datetime import date
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from clickhouse_driver import Client


def get_ch_client() -> Client:
    """
    Создаёт клиент ClickHouse.
    По умолчанию ходим на localhost:9000 (порт проброшен из docker-compose Airflow).
    При необходимости можно переопределить через переменные окружения.
    """
    return Client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "9000")),
        user=os.getenv("CLICKHOUSE_USER", "airflow"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "airflow"),
        database=os.getenv("CLICKHOUSE_DB", "bionicpro"),
        settings={"use_numpy": False},
    )


class DailyTelemetry(BaseModel):
    event_date: date
    prosthesis_id: str
    events: int
    err_events: int
    avg_response_ms: float
    p95_response_ms: float
    avg_battery_level: float


class UserReport(BaseModel):
    customer_id: str
    full_name: str
    country: str
    days: List[DailyTelemetry]


app = FastAPI(
    title="BionicPRO Reports API",
    description="Сервис выдачи отчётов по данным из OLAP (ClickHouse)",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/reports", response_model=UserReport)
def get_report(
    customer_id: Optional[str] = Query(
        default=None,
        description="ID клиента (customer_id) в витрине user_daily_telemetry",
    ),
):
    """
    Возвращает готовый отчёт по пользователю из витрины bionicpro.user_daily_telemetry.
    - Читает только подготовленные данные (без тяжёлых агрегаций в рантайме).
    - Если customer_id не передан, используется тестовый клиент 'c1'.
    """

    cid = customer_id or "c1"

    client = get_ch_client()

    rows = client.execute(
        """
        SELECT
          event_date,
          prosthesis_id,
          events,
          err_events,
          avg_response_ms,
          p95_response_ms,
          avg_battery_level,
          customer_id,
          full_name,
          country
        FROM bionicpro.user_daily_telemetry
        WHERE customer_id = %(cid)s
        ORDER BY event_date, prosthesis_id
        """,
        {"cid": cid},
    )

    if not rows:
        raise HTTPException(status_code=404, detail="Report not found for given customer_id")

    first_row = rows[0]
    _, _, _, _, _, _, _, customer_id_value, full_name, country = first_row

    days: List[DailyTelemetry] = []
    for (
        event_date,
        prosthesis_id,
        events,
        err_events,
        avg_response_ms,
        p95_response_ms,
        avg_battery_level,
        _cid,
        _full_name,
        _country,
    ) in rows:
        days.append(
            DailyTelemetry(
                event_date=event_date,
                prosthesis_id=prosthesis_id,
                events=events,
                err_events=err_events,
                avg_response_ms=avg_response_ms,
                p95_response_ms=p95_response_ms,
                avg_battery_level=avg_battery_level,
            )
        )

    return UserReport(
        customer_id=customer_id_value,
        full_name=full_name,
        country=country,
        days=days,
    )
