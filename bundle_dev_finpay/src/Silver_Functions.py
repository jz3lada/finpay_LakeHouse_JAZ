# =============================================================================
# silver_functions.py
# Librería de funciones reutilizables — Capa Silver
# Fintech FinPay
#
# RESPONSABILIDADES:
#   - Casteos y normalización de campos (trim, fechas, montos)
#   - Deduplicación por PK + _ingestion_ts más reciente
#   - Construcción del registro de quarantine
#   - Escritura en tabla de quarantine (batch desde DLT)
#
# ESTE ARCHIVO NO SE EJECUTA DIRECTAMENTE.
# Es importado por cada pipeline DLT silver_<fuente>_dlt.py
# =============================================================================

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType, DateType, LongType
)
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("silver_functions")


# =============================================================================
# 1. NORMALIZACIÓN DE FECHAS
#    Soporta dos formatos de entrada: yyyy-MM-dd y dd/MM/yyyy.
#    El resultado siempre es DATE en formato yyyy-MM-dd.
#    Si ningún formato aplica → null (el EXPECT de DLT lo capturará).
# =============================================================================

def normalize_date(col_name: str, alias: str = None) -> F.Column:
    """
    Intenta parsear una columna de fecha en dos formatos:
        1. yyyy-MM-dd  (formato ISO)
        2. dd/MM/yyyy  (formato latino)

    Retorna un DateType. Si ambos fallan → null.

    Args:
        col_name : nombre de la columna de entrada (string sucio)
        alias    : nombre de salida (default: col_name)
    Returns:
        Column de tipo DateType
    """
    result = F.coalesce(
        F.try_to_date(F.trim(F.col(col_name)), "yyyy-MM-dd"),
        F.try_to_date(F.trim(F.col(col_name)), "dd/MM/yyyy")
    )
    return result.alias(alias or col_name)


# =============================================================================
# 2. NORMALIZACIÓN DE MONTO
#    El campo amount llega como STRING con posibles formatos sucios:
#    "1,234.56" / "1.234,56" / " 99.9 " / "USD 100" / "$50.00"
#    Limpieza:
#      - trim espacios
#      - quitar símbolos de moneda y letras
#      - normalizar separador decimal (coma → punto)
#      - cast a DOUBLE
#      - Si resultado <= 0 o no casteable → null (EXPECT lo capturará)
# =============================================================================

def normalize_amount(col_name: str, alias: str = None) -> F.Column:
    """
    Limpia y castea el campo amount de STRING a DOUBLE.
    Elimina símbolos, normaliza separador decimal.
    Retorna null si el resultado no es numérico positivo.

    Args:
        col_name : nombre de la columna de entrada
        alias    : nombre de salida (default: col_name)
    Returns:
        Column de tipo DoubleType
    """
    cleaned = (
        F.regexp_replace(
            F.regexp_replace(
                F.trim(F.col(col_name)),
                "[^0-9.,]", ""          # quita todo excepto dígitos, punto y coma
            ),
            ",", "."                    # normaliza coma decimal a punto
        )
    )
    # Si hay más de un punto (ej: "1.234.56") tomar solo la última parte
    # Estrategia: quitar todos los puntos excepto el último
    normalized = F.regexp_replace(
        cleaned,
        r"\.(?=.*\.)", ""              # elimina puntos que no son el último
    )
    casted = normalized.cast("double")
    # Retorna null si <= 0 (monto no puede ser negativo en bronze incoming)
    result = F.when(casted > 0, casted).otherwise(F.lit(None).cast("double"))
    return result.alias(alias or col_name)


# =============================================================================
# 3. NORMALIZACIÓN DE CAMPOS TEXTO
#    trim + lower para campos categóricos controlados
#    trim solo para IDs y campos de formato fijo
# =============================================================================

def trim_col(col_name: str, alias: str = None) -> F.Column:
    """Aplica trim a una columna string."""
    return F.trim(F.col(col_name)).alias(alias or col_name)


def trim_lower_col(col_name: str, alias: str = None) -> F.Column:
    """Aplica trim + lower a una columna string categórica."""
    return F.lower(F.trim(F.col(col_name))).alias(alias or col_name)


# =============================================================================
# 4. LIMPIEZA COMPLETA DE TRANSACTIONS
#    Aplica todas las transformaciones de normalización a las columnas
#    de la fuente transactions.
#    Retorna el DataFrame con los campos limpios y casteados.
#    Los campos inválidos quedan como null — DLT EXPECT los detectará.
# =============================================================================

def clean_transactions(df: DataFrame) -> DataFrame:
    """
    Aplica limpieza y normalización a todas las columnas de transactions.

    Transformaciones:
        transaction_id   : trim
        user_id          : trim
        merchant_id      : trim
        channel          : trim + lower
        transaction_type : trim + lower
        amount           : normalize_amount → DOUBLE
        currency         : trim + upper
        transaction_date : normalize_date → DATE (yyyy-MM-dd)
        status           : trim + lower
        reference_id     : trim (puede ser null — es válido para pagos/retiros)

    Args:
        df: DataFrame de bronze.transactions (con columnas de auditoría _*)
    Returns:
        DataFrame con columnas normalizadas
    """
    return df.select(
        # --- Campos de negocio limpios ---
        trim_col("transaction_id"),
        trim_col("user_id"),
        trim_col("merchant_id"),
        trim_lower_col("channel"),
        trim_lower_col("transaction_type"),
        normalize_amount("amount"),
        F.upper(F.trim(F.col("currency"))).alias("currency"),
        normalize_date("transaction_date"),
        trim_lower_col("status"),
        F.when(
            F.trim(F.col("reference_id")) == "", F.lit(None)
        ).otherwise(
            F.trim(F.col("reference_id"))
        ).alias("reference_id"),

        # --- Columnas de auditoría heredadas de bronze ---
        F.col("_source_name"),
        F.col("_source_format"),
        F.col("_schema_version"),
        F.col("_ingestion_ts"),
        F.col("_source_file"),
    )


# =============================================================================
# 5. DEDUPLICACIÓN POR PK + _ingestion_ts MÁS RECIENTE
#    Estrategia: Window particionado por PK, ordenado por _ingestion_ts DESC.
#    Se queda con row_number == 1 → el registro más reciente por PK.
#    Aplicar ANTES de las validaciones de calidad.
# =============================================================================

def deduplicate_by_latest(df: DataFrame, pk_columns: list[str]) -> DataFrame:
    """
    Deduplica el DataFrame manteniendo el registro más reciente
    por PK según _ingestion_ts.

    Args:
        df         : DataFrame limpio con _ingestion_ts
        pk_columns : lista de columnas que forman la PK (ej: ["transaction_id"])
    Returns:
        DataFrame deduplicado — un registro por PK (el más reciente)
    """
    if not pk_columns:
        raise ValueError("pk_columns no puede estar vacío para deduplicación.")

    window = Window.partitionBy(*pk_columns).orderBy(F.col("_ingestion_ts").desc())

    return (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

# =============================================================================
# 8. WRITER EVENT LOG SILVER
#    Persiste el resultado del pipeline silver en observability.
#    Mismo schema que bronze para mantener consistencia en el dashboard.
# =============================================================================

def write_silver_event_log(
    spark: SparkSession,
    source_name: str,
    target_table: str,
    status: str,
    records_processed: int,
    records_quarantined: int,
    duration_sec: float,
    pipeline_run_id: str,
    notebook_path: str = "unknown",
    error_message: str = None,
) -> None:
    """
    Escribe un registro en fintech_finpay.observability.pipeline_event_log
    para el pipeline silver de una fuente.

    Args:
        spark               : SparkSession activa
        source_name         : nombre de la fuente
        target_table        : tabla silver destino
        status              : SUCCESS | FAILED
        records_processed   : registros que pasaron todas las validaciones
        records_quarantined : registros enviados a quarantine
        duration_sec        : duración del pipeline
        pipeline_run_id     : UUID del run
        notebook_path       : path del notebook DLT
        error_message       : detalle si status=FAILED
    """
    from datetime import datetime, timezone
    import uuid
    from pyspark.sql.types import (
        StructType, StructField, StringType, LongType,
        DoubleType, IntegerType, DateType, TimestampType
    )
    from pyspark.sql import Row

    OBSERVABILITY_TABLE = "fintech_finpay.observability.pipeline_event_log"
    now = datetime.now(timezone.utc)

    EVENT_LOG_SCHEMA = StructType([
        StructField("event_id",          StringType(),    nullable=False),
        StructField("pipeline_run_id",   StringType(),    nullable=False),
        StructField("event_ts",          TimestampType(), nullable=False),
        StructField("event_date",        DateType(),      nullable=False),
        StructField("layer",             StringType(),    nullable=True),
        StructField("source_name",       StringType(),    nullable=True),
        StructField("target_table",      StringType(),    nullable=True),
        StructField("file_format",       StringType(),    nullable=True),
        StructField("status",            StringType(),    nullable=True),
        StructField("records_processed", LongType(),      nullable=True),
        StructField("records_failed",    LongType(),      nullable=True),
        StructField("error_message",     StringType(),    nullable=True),
        StructField("duration_sec",      DoubleType(),    nullable=True),
        StructField("write_mode",        StringType(),    nullable=True),
        StructField("dedup_mode",        StringType(),    nullable=True),
        StructField("schema_version",    IntegerType(),   nullable=True),
        StructField("source_filter",     StringType(),    nullable=True),
        StructField("notebook_path",     StringType(),    nullable=True),
    ])

    row = Row(
        event_id          = str(uuid.uuid4()),
        pipeline_run_id   = pipeline_run_id,
        event_ts          = now,
        event_date        = now.date(),
        layer             = "silver",
        source_name       = source_name,
        target_table      = target_table,
        file_format       = "delta",          # silver siempre lee desde delta
        status            = status,
        records_processed = int(records_processed),
        records_failed    = int(records_quarantined),
        error_message     = error_message,
        duration_sec      = float(duration_sec),
        write_mode        = "streaming_append",
        dedup_mode        = "latest_by_ingestion_ts",
        schema_version    = 1,
        source_filter     = source_name,
        notebook_path     = notebook_path,
    )

    df_event = spark.createDataFrame([row], schema=EVENT_LOG_SCHEMA)

    (
        df_event.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(OBSERVABILITY_TABLE)
    )

    logger.info(
        f"  [OBS] Event log silver persistido → {OBSERVABILITY_TABLE} | "
        f"status={status} | processed={records_processed:,} | quarantined={records_quarantined:,}"
    )