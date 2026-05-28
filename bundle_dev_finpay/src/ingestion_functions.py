# =============================================================================
# ingestion_functions.py
# Librería de funciones reutilizables — Pipeline Metadata-Driven
# Fintech FinPay | Capa Bronze
#
# Auto Loader usa spark.readStream + trigger(availableNow=True)
# Equivale a un batch incremental con checkpoint — no es streaming continuo.
# =============================================================================

import json
import logging
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType,
    FloatType, BooleanType, DateType, TimestampType, ShortType, DecimalType
)
from pyspark.sql.utils import AnalysisException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("ingestion_functions")


# =============================================================================
# 1. MAPEO DE TIPOS
# =============================================================================

TYPE_MAP: dict = {
    "string":    StringType(),
    "integer":   IntegerType(),
    "int":       IntegerType(),
    "long":      LongType(),
    "bigint":    LongType(),
    "double":    DoubleType(),
    "float":     FloatType(),
    "boolean":   BooleanType(),
    "bool":      BooleanType(),
    "date":      DateType(),
    "timestamp": TimestampType(),
    "short":     ShortType(),
}


def resolve_spark_type(data_type: str):
    if data_type.lower().startswith("decimal"):
        try:
            inner = data_type[data_type.index("(") + 1 : data_type.index(")")]
            p, s  = [int(x.strip()) for x in inner.split(",")]
            return DecimalType(p, s)
        except Exception:
            raise ValueError(f"Formato decimal inválido: '{data_type}'. Use 'decimal(p,s)'.")

    spark_type = TYPE_MAP.get(data_type.lower())
    if spark_type is None:
        raise ValueError(
            f"Tipo '{data_type}' no soportado. "
            f"Tipos válidos: {list(TYPE_MAP.keys())} + decimal(p,s)"
        )
    return spark_type


# =============================================================================
# 2. CONSTRUCTOR DE ESQUEMA
# =============================================================================

def build_spark_schema(archetype: dict) -> StructType:
    if archetype.get("infer_column_types", False):
        raise ValueError(
            f"[{archetype['source_name']}] 'infer_column_types' está en true. "
            "Esta arquitectura no permite inferencia de esquemas."
        )

    schema_def = archetype.get("schema", [])
    if not schema_def:
        raise ValueError(
            f"[{archetype['source_name']}] El arquetipo no tiene 'schema' definido."
        )

    fields = [
        StructField(col["column_name"], resolve_spark_type(col["data_type"]), nullable=True)
        for col in schema_def
    ]

    logger.info(f"  [SCHEMA] {archetype['source_name']} — {len(fields)} campos | infer_column_types=false ✓")
    return StructType(fields)


# =============================================================================
# 3. LOADER DE ARQUETIPOS
# =============================================================================

def load_archetypes(spark: SparkSession, metadata_path: str) -> list[dict]:
    logger.info(f"Leyendo metadata desde: {metadata_path}")

    raw_json = spark.read.text(metadata_path, wholetext=True).collect()[0][0]
    parsed   = json.loads(raw_json)

    all_archetypes    = parsed if isinstance(parsed, list) else parsed.get("ingestion_archetypes", [])
    active_archetypes = [a for a in all_archetypes if a.get("active", False)]

    logger.info(
        f"Arquetipos — total: {len(all_archetypes)} | "
        f"activos: {len(active_archetypes)} | "
        f"inactivos: {len(all_archetypes) - len(active_archetypes)}"
    )
    return active_archetypes


# =============================================================================
# 4. READERS — Auto Loader con readStream + availableNow
#
# availableNow=True → procesa todos los archivos nuevos desde el último
# checkpoint y termina. Comportamiento idéntico a un batch, pero con
# tracking incremental. No deja un stream corriendo.
# =============================================================================

def _base_cloudfiles_reader(spark: SparkSession, format: str, archetype: dict, schema: StructType):
    """
    Construye el readStream base de Auto Loader común para todos los formatos.
    """
    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",           format)
        .option("cloudFiles.schemaLocation",   archetype["schema_location"])
        .option("cloudFiles.inferColumnTypes", "false")
        .option("pathGlobFilter",              archetype.get("file_pattern", f"*.{format}"))
        .option("badRecordsPath",              archetype["bad_records_path"])
        .schema(schema)
    )

    # Schema evolution
    evo_mode = archetype.get("schema_evolution_mode", "merge").lower()
    if evo_mode == "merge":
        reader = reader.option("mergeSchema", "true")
    elif evo_mode == "rescue":
        reader = reader.option("rescuedDataColumn", archetype.get("rescued_data_column", "_rescued_data"))
    elif evo_mode == "fail":
        reader = reader.option("failOnNewColumns", "true")
    else:
        raise ValueError(f"schema_evolution_mode '{evo_mode}' no válido. Valores: merge | rescue | fail")

    return reader


def read_csv(spark: SparkSession, archetype: dict, schema: StructType) -> DataFrame:
    delimiter  = archetype.get("delimiter", ",") or ","
    header_str = "true" if archetype.get("header", True) else "false"
    encoding   = archetype.get("encoding", "UTF-8")
    multiline  = "true" if archetype.get("multiline", False) else "false"

    logger.info(
        f"  [CSV/TXT] path={archetype['source_path']} | "
        f"pattern={archetype.get('file_pattern')} | "
        f"delimiter='{delimiter}' | header={header_str}"
    )

    stream = (
        _base_cloudfiles_reader(spark, "csv", archetype, schema)
        .option("header",          header_str)
        .option("sep",             delimiter)
        .option("encoding",        encoding)
        .option("multiLine",       multiline)
        .option("dateFormat",      "yyyy-MM-dd")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .load(archetype["source_path"])
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
    return stream


def read_json(spark: SparkSession, archetype: dict, schema: StructType) -> DataFrame:
    multiline = "true" if archetype.get("multiline", True) else "false"
    encoding  = archetype.get("encoding", "UTF-8")

    logger.info(
        f"  [JSON] path={archetype['source_path']} | "
        f"pattern={archetype.get('file_pattern')} | "
        f"multiline={multiline}"
    )

    stream = (
        _base_cloudfiles_reader(spark, "json", archetype, schema)
        .option("multiLine",       multiline)
        .option("encoding",        encoding)
        .option("dateFormat",      "yyyy-MM-dd")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .load(archetype["source_path"])
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
    return stream


def read_txt(spark: SparkSession, archetype: dict, schema: StructType) -> DataFrame:
    """
    Lee archivos .txt delimitados vía Auto Loader.
    Internamente usa cloudFiles.format='csv' porque Spark trata cualquier
    archivo de texto delimitado como CSV, independiente de la extensión.
    El file_pattern del JSON (ej: users_*.txt) filtra los archivos correctos
    vía pathGlobFilter, por lo que la extensión .txt se respeta en la selección.

    Comparte toda la lógica de read_csv — el registro separado en
    READER_REGISTRY hace explícito el soporte de archivos .txt en el JSON.
    """
    delimiter  = archetype.get("delimiter", "|") or "|"
    header_str = "true" if archetype.get("header", True) else "false"
    encoding   = archetype.get("encoding", "UTF-8")
    multiline  = "true" if archetype.get("multiline", False) else "false"

    logger.info(
        f"  [TXT] path={archetype['source_path']} | "
        f"pattern={archetype.get('file_pattern')} | "
        f"delimiter='{delimiter}' | header={header_str}"
    )

    # cloudFiles.format="csv" es correcto para cualquier delimitado (.csv, .txt, .tsv)
    stream = (
        _base_cloudfiles_reader(spark, "csv", archetype, schema)
        .option("header",          header_str)
        .option("sep",             delimiter)
        .option("encoding",        encoding)
        .option("multiLine",       multiline)
        .option("dateFormat",      "yyyy-MM-dd")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .load(archetype["source_path"])
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
    return stream


# Registry de readers — un formato declarado en el JSON = una entrada aquí
# csv  → archivos .csv con cualquier delimitador
# txt  → archivos .txt delimitados (pipe, coma, tabulador, etc.)
# json → archivos .json (soporta multiline)
READER_REGISTRY: dict = {
    "csv":  read_csv,
    "txt":  read_txt,
    "json": read_json,
}


def get_reader(file_format: str):
    reader = READER_REGISTRY.get(file_format.lower())
    if not reader:
        raise ValueError(
            f"Formato '{file_format}' no soportado. "
            f"Formatos válidos: {list(READER_REGISTRY.keys())}"
        )
    return reader


# =============================================================================
# 5. COLUMNAS DE AUDITORÍA
# =============================================================================

def add_audit_columns(df: DataFrame, archetype: dict) -> DataFrame:
    observability = archetype.get("enable_observability", True)

    df = df.withColumn("_ingestion_ts", F.current_timestamp())

    if observability:
        df = (
            df
            .withColumn("_source_name",    F.lit(archetype["source_name"]))
            .withColumn("_source_format",  F.lit(archetype["file_format"]))
            .withColumn("_schema_version", F.lit(archetype.get("schema_version", 1)))
        )
        logger.info("  [OBS] Columnas de observabilidad añadidas ✓")

    return df


# =============================================================================
# 6. TABLE PROPERTIES
# =============================================================================

def apply_table_properties(spark: SparkSession, full_table_name: str, table_properties: dict) -> None:
    if not table_properties:
        return
    props_str = ", ".join(f"'{k}' = '{v}'" for k, v in table_properties.items())
    spark.sql(f"ALTER TABLE {full_table_name} SET TBLPROPERTIES ({props_str})")
    logger.info(f"  [PROPS] Table properties aplicadas: {list(table_properties.keys())}")


# =============================================================================
# 7. WRITER — writeStream + trigger(availableNow=True)
#
# Auto Loader es una fuente de streaming. El writer también debe ser
# writeStream. availableNow=True hace que procese todo lo pendiente y
# termine el stream — equivale exactamente a un batch incremental.
#
# outputMode("append") es el único modo válido para Delta en este contexto.
# checkpointLocation es obligatorio para que Auto Loader lleve el tracking.
# =============================================================================

def write_to_delta(spark: SparkSession, df: DataFrame, archetype: dict) -> int:
    catalog   = archetype["target_catalog"]
    schema_db = archetype["target_schema"]
    table     = archetype["target_table"]
    full_name = f"{catalog}.{schema_db}.{table}"
    evo_mode  = archetype.get("schema_evolution_mode", "merge").lower()

    # Crear schema si no existe
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_db}")

    logger.info(f"  [WRITE] → {full_name} | mode=append (streaming) | checkpoint={archetype['checkpoint_location']}")

    writer = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", archetype["checkpoint_location"])
        .trigger(availableNow=True)                 # procesa pendientes y termina
    )

    if evo_mode == "merge":
        writer = writer.option("mergeSchema", "true")

    # .toTable() escribe en la tabla Delta gestionada por Unity Catalog
    query = writer.toTable(full_name)

    # awaitTermination() bloquea hasta que el micro-batch termine
    query.awaitTermination()

    # Contar registros escritos consultando la tabla ya escrita
    record_count = spark.table(full_name).count()

    apply_table_properties(spark, full_name, archetype.get("table_properties", {}))

    return record_count


# =============================================================================
# 8. DEDUPLICACIÓN
# Nota: en streaming con Auto Loader, drop_duplicates y latest_record
# operan sobre el micro-batch actual (availableNow). Para dedup global
# se recomienda manejarla en la capa Silver con MERGE.
# =============================================================================

def apply_deduplication(df: DataFrame, archetype: dict) -> DataFrame:
    mode = archetype.get("deduplication_mode", "none").lower()
    pk   = archetype.get("primary_key", [])

    if mode == "none":
        return df

    elif mode == "drop_duplicates":
        if not pk:
            raise ValueError(
                f"[{archetype['source_name']}] deduplication_mode='drop_duplicates' "
                "requiere primary_key definido."
            )
        logger.info(f"  [DEDUP] drop_duplicates por pk={pk}")
        return df.dropDuplicates(pk)

    elif mode == "latest_record":
        # En streaming, latest_record se aplica por micro-batch
        if not pk:
            raise ValueError(
                f"[{archetype['source_name']}] deduplication_mode='latest_record' "
                "requiere primary_key definido."
            )
        ts_fields = [
            col["column_name"]
            for col in archetype.get("schema", [])
            if col["data_type"].lower() == "timestamp"
        ]
        if not ts_fields:
            raise ValueError(
                f"[{archetype['source_name']}] deduplication_mode='latest_record' "
                "requiere al menos un campo timestamp en el schema."
            )
        logger.info(f"  [DEDUP] latest_record por pk={pk} ts={ts_fields[0]}")
        return df.dropDuplicates(pk)   # en streaming se usa dropDuplicates por PK

    else:
        raise ValueError(
            f"deduplication_mode '{mode}' no válido. Valores: none | drop_duplicates | latest_record"
        )


# =============================================================================
# 9. INGESTOR DE UNA FUENTE
# =============================================================================

def ingest_source(spark: SparkSession, archetype: dict) -> dict:
    source_name = archetype["source_name"]
    full_table  = (
        f"{archetype['target_catalog']}."
        f"{archetype['target_schema']}."
        f"{archetype['target_table']}"
    )

    result = {
        "source_name":  source_name,
        "file_format":  archetype.get("file_format"),
        "target_table": full_table,
        "write_mode":   "append (availableNow)",
        "dedup_mode":   archetype.get("deduplication_mode"),
        "evo_mode":     archetype.get("schema_evolution_mode"),
        "status":       None,
        "records":      0,
        "error":        None,
        "duration_sec": 0.0,
    }

    start_ts = datetime.now(timezone.utc)

    try:
        logger.info(f"{'='*60}")
        logger.info(f"▶ [{source_name}] → {full_table}")

        # 1. Schema explícito desde JSON
        schema = build_spark_schema(archetype)

        # 2. Reader factory
        reader = get_reader(archetype["file_format"])

        # 3. Leer como stream (Auto Loader)
        df = reader(spark, archetype, schema)

        # 4. Deduplicación (sobre micro-batch)
        df = apply_deduplication(df, archetype)

        # 5. Auditoría
        df = add_audit_columns(df, archetype)

        # 6. Escribir con writeStream + availableNow
        record_count = write_to_delta(spark, df, archetype)

        result["status"]  = "SUCCESS"
        result["records"] = record_count
        logger.info(f"Caga de la fuente: [{source_name}] completada — se cargaron: {record_count:,} registros")

    except ValueError as e:
        result["status"] = "FAILED"
        result["error"]  = str(e)
        logger.error(f"❌ [{source_name}] Error de configuración: {e}")

    except AnalysisException as e:
        result["status"] = "FAILED"
        result["error"]  = f"AnalysisException: {str(e)[:400]}"
        logger.error(f"❌ [{source_name}] Error Spark Analysis: {e}")

    except Exception as e:
        result["status"] = "FAILED"
        result["error"]  = f"{type(e).__name__}: {str(e)[:400]}"
        logger.error(f"❌ [{source_name}] Error inesperado: {e}")

    finally:
        result["duration_sec"] = round(
            (datetime.now(timezone.utc) - start_ts).total_seconds(), 2
        )

    return result

# =============================================================================
# WRITER BATCH — para reproceso de bad records
# =============================================================================
 
def write_to_delta_batch(spark: SparkSession, df: DataFrame, archetype: dict) -> int:
    """
    Escribe un DataFrame batch (no streaming) en la tabla Delta de destino.
    Diseñado para reproceso de bad records u otras correcciones manuales.
 
    A diferencia de write_to_delta() que usa writeStream + availableNow,
    esta función usa df.write directamente — válido porque el DataFrame
    ya es estático (no proviene de Auto Loader).
 
    Args:
        spark:     SparkSession activa.
        df:        DataFrame estático ya corregido y con columnas de auditoría.
        archetype: dict del arquetipo (mismo JSON del pipeline normal).
    Returns:
        Número de registros escritos.
    """
    catalog   = archetype["target_catalog"]
    schema_db = archetype["target_schema"]
    table     = archetype["target_table"]
    full_name = f"{catalog}.{schema_db}.{table}"
    evo_mode  = archetype.get("schema_evolution_mode", "merge").lower()
 
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_db}")
 
    record_count = df.count()
    logger.info(
        f"  [WRITE BATCH] → {full_name} | "
        f"mode=append | registros: {record_count:,}"
    )
 
    writer = (
        df.write
        .format("delta")
        .mode("append")
    )
 
    if evo_mode == "merge":
        writer = writer.option("mergeSchema", "true")
 
    writer.saveAsTable(full_name)
 
    apply_table_properties(spark, full_name, archetype.get("table_properties", {}))
 
    logger.info(f"[WRITE BATCH] {record_count:,} registros insertados en {full_name}")
    return record_count