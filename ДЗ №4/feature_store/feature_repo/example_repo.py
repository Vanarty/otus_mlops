# Это пример файла с определением признаков (feature definition)

import os

from datetime import timedelta

import numpy as np
import pandas as pd

from feast import (
    Entity,
    FeatureService,
    FeatureView,
    Field,
    FileSource,
    PushSource,
    RequestSource
)
from feast.feature_logging import LoggingConfig
from feast.infra.offline_stores.file_source import FileLoggingDestination
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int32, Int64

REPO_PATH = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(REPO_PATH, "data")

# Определяем сущность для водителя. Сущность можно рассматривать как первичный ключ,
# который используется для получения признаков
customer = Entity(name="customer_id", join_keys=["customer_id"])

# Читаем данные из parquet файлов. Parquet удобен для локальной разработки.
# Для промышленного использования можно использовать любое хранилище данных,
# например BigQuery. Подробнее смотрите в документации Feast
daily_transaction_source = FileSource(
    name="driver_daily_transaction_source",
    path=os.path.join(DATA_PATH, "fraud_snap.parquet"),
    timestamp_field="event_timestamp",
)

# Наши parquet файлы содержат примеры данных, включающие столбец customer_id,
# временные метки и столбцы с признаками. Здесь мы определяем Feature View,
# который позволит нам передавать эти данные в нашу модель онлайн
customer_transaction_fv = FeatureView(
    # Уникальное имя этого представления признаков. Два представления признаков
    # в одном проекте не могут иметь одинаковое имя
    name="driver_daily_transaction",
    entities=[customer],
    ttl=timedelta(days=30), # Время жизни фичей
    # Список признаков, определенных ниже, действует как схема для материализации
    # признаков в хранилище, а также используется как ссылки при извлечении
    # для создания обучающего набора данных или предоставления признаков
    schema=[
        Field(name="transaction_id", dtype=Int32),
        Field(name="terminal_id", dtype=Int32),
        Field(name="tx_amount", dtype=Float32),
        Field(name="tx_time_seconds", dtype=Int32),
        Field(name="tx_time_days", dtype=Int32),
        Field(name="tx_fraud", dtype=Int32),
        Field(name="tx_fraud_scenario", dtype=Int32),
    ],
    online=True,
    source=daily_transaction_source,
    # Теги - это определенные пользователем пары ключ/значение,
    # которые прикрепляются к каждому представлению признаков
    tags={"team": "daily_transaction"},
)

# моя агрегированная фича по запросу (Materialized Feature Views)
@on_demand_feature_view(
    sources=[customer_transaction_fv],
    schema=[Field(name="avg_tx_amount_last_7d", dtype=Float64)]
)
def compute_avg_tx_amount_last_7d(features_df: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["avg_tx_amount_last_7d"] = features_df["tx_amount"].rolling(window=7).mean()
    return df


# Определяем источник данных запроса, который кодирует признаки/информацию,
# доступную только во время запроса (например, часть пользовательского HTTP-запроса)
input_request = RequestSource(
    name="vals_to_add",
    schema=[
        Field(name="val_to_add", dtype=Int64),
        Field(name="val_to_add_2", dtype=Int64),
    ],
)

# Определяем представление признаков по требованию, которое может генерировать
# новые признаки на основе существующих представлений и признаков из RequestSource
@on_demand_feature_view(
    sources=[customer_transaction_fv, input_request],
    schema=[
        Field(name="tx_fraud_plus_val1", dtype=Int64),
        Field(name="tx_fraud_scenario_plus_val2", dtype=Int64),
    ],
)
def transformed_tx_fraud(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["tx_fraud_plus_val1"] = inputs["tx_fraud"] + inputs["val_to_add"]
    df["tx_fraud_scenario_plus_val2"] = inputs["tx_fraud_scenario"] + inputs["val_to_add_2"]
    return df


# FeatureService группирует признаки в версию модели
transaction_activity_v1 = FeatureService(
    name="daily_transaction_v1",
    features=[
        customer_transaction_fv[["terminal_id", "transaction_id"]],  # Выбирает подмножество признаков из представления
        transformed_tx_fraud,  # Выбирает все признаки из представления
    ],
    logging_config=LoggingConfig(
        destination=FileLoggingDestination(path=DATA_PATH)
    ),
)
transaction_activity_v2 = FeatureService(
    name="transaction_activity_v2", features=[customer_transaction_fv, transformed_tx_fraud]
)

# Определяет способ отправки данных (доступных офлайн, онлайн или обоих типов) в Feast
daily_transaction_push_source = PushSource(
    name="daily_transaction_push_source",
    batch_source=daily_transaction_source,
)

# Определяет слегка измененную версию представления признаков, описанного выше,
# где источник был изменен на push source. Это позволяет напрямую отправлять
# свежие признаки в онлайн-хранилище для этого представления признаков
daily_transaction_fresh_fv = FeatureView(
    name="daily_transaction_stats_fresh",
    entities=[customer],
    ttl=timedelta(days=1),
    schema=[
        Field(name="transaction_id", dtype=Int32),
        Field(name="terminal_id", dtype=Int32),
        Field(name="tx_amount", dtype=Float32),
        Field(name="tx_time_seconds", dtype=Int32),
        Field(name="tx_time_days", dtype=Int32),
        Field(name="tx_fraud", dtype=Int32),
        Field(name="tx_fraud_scenario", dtype=Int32),
    ],
    online=True,
    source=daily_transaction_push_source,  # Изменено по сравнению с предыдущей версией
    tags={"team": "daily_transaction"},
)


# Определяем представление признаков по требованию, которое может генерировать
# новые признаки на основе существующих представлений и признаков из RequestSource
@on_demand_feature_view(
    sources=[daily_transaction_fresh_fv, input_request],  # использует свежую версию Feature View
    schema=[
        Field(name="tx_fraud_plus_val1", dtype=Int64),
        Field(name="tx_fraud_scenario_plus_val2", dtype=Int64),
    ],
)
def transformed_tx_fraud_fresh(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["tx_fraud_plus_val1"] = inputs["tx_fraud"] + inputs["val_to_add"]
    df["tx_fraud_scenario_plus_val2"] = inputs["tx_fraud_scenario"] + inputs["val_to_add_2"]
    return df


transaction_activity_v3 = FeatureService(
    name="transaction_activity_v3",
    features=[daily_transaction_fresh_fv, transformed_tx_fraud_fresh],
)

# Добавлено!
@on_demand_feature_view(
    sources=[customer_transaction_fv],  # Используем существующий Feature View
    schema=[
        Field(name="combined_time", dtype=Float64)
    ],
)
def customer_performance_metrics(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()

    # Рассчитываем комбинированный рейтинг как взвешенную сумму
    # tx_time_days имеет вес 0.6, tx_time_seconds имеет вес 0.4
    df["combined_time"] = (inputs["tx_time_days"] * 0.6 + inputs["tx_time_seconds"] * 0.4)

    return df

# Обновляем существующий FeatureService, добавляя новые метрики
transaction_activity_v4 = FeatureService(
    name="transaction_activity_v4",
    features=[
        customer_transaction_fv,
        customer_performance_metrics  # Добавляем новые метрики в сервис
    ]
)
