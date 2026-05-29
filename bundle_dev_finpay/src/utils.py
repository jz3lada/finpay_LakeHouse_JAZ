# src/utils.py
# FinPay Lakehouse - utilidades compartidas
#
# Arquitectura final:
#   Landing files
#      -> AUTO CDC SCD Type 1
#      -> Silver current tables
#
# Principios:
# - ingestion_archetypes.json controla rutas, formato, delimitador, schemaLocation y active.
# - NO usamos _change_type porque es columna reservada por Delta Change Data Feed.
# - Usamos _cdc_operation como operación CDC-like propia del proyecto.
# - Silver solo persiste current tables y quarantine.

from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType


DEFAULT_CATALOG = "fintech_finpay"
DEFAULT_DEFAULT_SCHEMA = "default"
DEFAULT_LANDING_VOLUME = "vol_landing"


# ============================================================
# Configuración metadata-driven
# ============================================================

def get_catalog(spark: SparkSession) -> str:
    return spark.conf.get("finpay.catalog", DEFAULT_CATALOG)


def get_landing_path(spark: SparkSession) -> str:
    catalog = get_catalog(spark)
    default_schema = spark.conf.get("finpay.default_schema", DEFAULT_DEFAULT_SCHEMA)
    landing_volume = spark.conf.get("finpay.landing_volume", DEFAULT_LANDING_VOLUME)
    return f"/Volumes/{catalog}/{default_schema}/{landing_volume}"


def get_archetypes_path(spark: SparkSession) -> str:
    default_path = f"{get_landing_path(spark)}/metadata/ingestion_archetypes.json"
    return spark.conf.get("finpay.ingestion_archetypes_path", default_path)

def _bool_as_str(value: Optional[bool], default: str = "false") -> str:
    if value is None:
        return default
    return str(bool(value)).lower()

# ============================================================
# Silver valid changes: catálogos
# ============================================================

VALID_COUNTRIES = ["PE", "CO", "MX", "CL", "AR", "EC", "BR"]

VALID_CHANNELS = ["web", "app", "pos"]
VALID_TRANSACTION_TYPES = ["pago", "reversa", "retiro"]
VALID_TRANSACTION_STATUS = ["aprobado", "rechazado", "pendiente"]
VALID_CURRENCIES = ["PEN", "USD", "COP", "MXN", "CLP", "ARS"]

VALID_MERCHANT_CATEGORIES = [
    "retail",
    "restaurante",
    "farmacia",
    "supermercado",
    "tecnologia",
    "transporte",
    "educacion",
    "salud",
    "entretenimiento",
    "moda",
]
VALID_MERCHANT_STATUS = ["activo", "inactivo", "suspendido"]
VALID_RISK_LEVELS = ["bajo", "medio", "alto"]

VALID_USER_SEGMENTS = ["premium", "estandar", "nuevo"]


# ============================================================
# Normalización y casteo
# ============================================================

def clean_string(column_name: str):
    value = F.trim(F.col(column_name).cast("string"))

    return (
        F.when(value.isNull(), F.lit(None).cast("string"))
        .when(value == "", F.lit(None).cast("string"))
        .when(
            F.upper(value).isin(
                "N/A",
                "NA",
                "NULL",
                "NONE",
                "NODISPONIBLE",
                "NO DISPONIBLE",
                "NO_DISPONIBLE",
                "SIN_CORREO",
                "SIN CORREO",
                "SIN NOMBRE",
                "DESCONOCIDO",
            ),
            F.lit(None).cast("string"),
        )
        .otherwise(value)
    )


def lower_clean(column_name: str):
    return F.lower(clean_string(column_name))


def parse_date_multi(column_name: str):
    value = clean_string(column_name)

    return F.coalesce(
        F.to_date(value, "yyyy-MM-dd"),
        F.to_date(value, "dd/MM/yyyy"),
        F.to_date(value, "yyyy/MM/dd"),
        F.to_date(value, "dd-MM-yyyy"),
    )


def parse_amount(column_name: str):
    value = F.regexp_replace(clean_string(column_name), r"[^0-9,.\-]", "")

    normalized = (
        F.when(value.rlike(r"^\d{1,3}(,\d{3})+(\.\d+)?$"), F.regexp_replace(value, ",", ""))
        .when(value.rlike(r"^\d+,\d{1,2}$"), F.regexp_replace(value, ",", "."))
        .otherwise(value)
    )

    return normalized.cast("decimal(18,2)")


def normalize_country_expr(column_name: str):
    value = F.upper(F.trim(clean_string(column_name)))

    return (
        F.when(value.isNull(), F.lit(None).cast("string"))
        .when(value.isin("PE", "PER", "PERU", "PERÚ"), F.lit("PE"))
        .when(value.isin("CO", "COL", "COLOMBIA"), F.lit("CO"))
        .when(value.isin("MX", "MEX", "MEXICO", "MÉXICO"), F.lit("MX"))
        .when(value.isin("CL", "CHI", "CHILE"), F.lit("CL"))
        .when(value.isin("AR", "ARG", "ARGENTINA"), F.lit("AR"))
        .when(value.isin("EC", "ECU", "ECUADOR"), F.lit("EC"))
        .when(value.isin("BR", "BRA", "BRASIL", "BRAZIL"), F.lit("BR"))
        .otherwise(value)
    )


def normalize_status_expr(column_name: str, allowed_values: List[str]):
    value = lower_clean(column_name)

    value = (
        F.when(value == "active", F.lit("activo"))
        .when(value == "suspended", F.lit("suspendido"))
        .otherwise(value)
    )

    return F.when(value.isin(*allowed_values), value).otherwise(value)


def normalize_risk_level_expr(column_name: str):
    value = lower_clean(column_name)

    return (
        F.when(value.isin("sin clasificar", "n/a", "na"), F.lit(None).cast("string"))
        .when(value.isin(*VALID_RISK_LEVELS), value)
        .otherwise(value)
    )


def normalize_segment_expr(column_name: str):
    value = lower_clean(column_name)

    return (
        F.when(value.isNull(), F.lit(None).cast("string"))
        .when(value.isin("standard", "std", "regular"), F.lit("estandar"))
        .when(value.isin("estándar", "estandar"), F.lit("estandar"))
        .when(value.isin("vip", "premium"), F.lit("premium"))
        .when(value.isin("new", "nuevo"), F.lit("nuevo"))
        .otherwise(value)
    )


def normalize_email_expr(column_name: str):
    return F.lower(clean_string(column_name))


def normalize_phone_expr(column_name: str):
    value = clean_string(column_name)
    digits = F.regexp_replace(value, r"[^0-9]", "")

    return (
        F.when(value.isNull(), F.lit(None).cast("string"))
        .when(value.startswith("+"), F.concat(F.lit("+"), digits))
        .when(digits.startswith("51"), F.concat(F.lit("+"), digits))
        .when(F.length(digits) == 9, F.concat(F.lit("+51"), digits))
        .otherwise(digits)
    )


def with_quality_errors(df: DataFrame, rules: Dict[str, object]) -> DataFrame:
    """
    Agrega quality_errors e is_valid.
    """
    error_items = [
        F.when(condition, F.lit(message)).otherwise(F.lit(None).cast("string"))
        for message, condition in rules.items()
    ]

    raw_errors = F.array(*error_items)
    clean_errors = F.filter(raw_errors, lambda error: error.isNotNull())

    return (
        df
        .withColumn("quality_errors", clean_errors)
        .withColumn("is_valid", F.size(F.col("quality_errors")) == 0)
    )


def add_processed_columns(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("_processed_at", F.current_timestamp())
        .withColumn("_load_date", F.current_date())
    )
