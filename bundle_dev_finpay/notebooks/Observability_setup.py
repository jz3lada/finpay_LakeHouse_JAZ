# =============================================================================
# observability_setup.py
# Script de inicialización — ejecutar UNA SOLA VEZ por ambiente.
# Crea el schema 'observability' y la tabla Delta de event logs
# en el catalog fintech_finpay.
#
# EJECUTAR ANTES de correr el pipeline por primera vez.
# =============================================================================

# COMMAND ----------
# MAGIC %md
# MAGIC # ⚙️ Setup Observability — fintech_finpay.observability
# MAGIC Ejecutar **una sola vez** por ambiente para crear la infraestructura
# MAGIC de event logs del pipeline de ingesta.

# COMMAND ----------
# =============================================================================
# CELDA 1 — CREAR SCHEMA OBSERVABILITY
# =============================================================================

spark.sql("CREATE SCHEMA IF NOT EXISTS fintech_finpay.observability")
print("✅ Schema fintech_finpay.observability listo")

# COMMAND ----------
# =============================================================================
# CELDA 2 — CREAR TABLA pipeline_event_log
#
# Columnas:
#   event_id          : UUID único por evento
#   pipeline_run_id   : UUID que agrupa todos los eventos de una misma ejecución
#   event_ts          : timestamp del evento (UTC)
#   event_date        : date del evento — para filtros por rango de fecha
#   layer             : capa del lakehouse (bronze / silver / gold)
#   source_name       : nombre de la fuente (del JSON)
#   target_table      : tabla destino completa (catalog.schema.table)
#   file_format       : formato del archivo fuente
#   status            : SUCCESS | FAILED
#   records_processed : registros escritos exitosamente
#   records_failed    : registros rechazados / bad records
#   error_message     : mensaje de error si status=FAILED (null si SUCCESS)
#   duration_sec      : duración de la ingesta de esa fuente
#   write_mode        : append / overwrite / merge / scd2
#   dedup_mode        : none / drop_duplicates / latest_record
#   schema_version    : versión del schema del JSON
#   source_filter     : parámetro source_name recibido por el Job (all / nombre)
#   notebook_path     : path del runner que generó el evento
# =============================================================================

spark.sql("""
    CREATE TABLE IF NOT EXISTS fintech_finpay.observability.pipeline_event_log (
        event_id          STRING         COMMENT 'UUID único por evento',
        pipeline_run_id   STRING         COMMENT 'UUID que agrupa todos los eventos de una ejecución',
        event_ts          TIMESTAMP      COMMENT 'Timestamp UTC del evento',
        event_date        DATE           COMMENT 'Fecha del evento para filtros por rango',
        layer             STRING         COMMENT 'Capa del Lakehouse: bronze | silver | gold',
        source_name       STRING         COMMENT 'Nombre de la fuente del JSON',
        target_table      STRING         COMMENT 'Tabla destino catalog.schema.table',
        file_format       STRING         COMMENT 'Formato del archivo fuente',
        status            STRING         COMMENT 'SUCCESS | FAILED',
        records_processed LONG           COMMENT 'Registros escritos exitosamente',
        records_failed    LONG           COMMENT 'Registros rechazados o bad records',
        error_message     STRING         COMMENT 'Detalle del error si status=FAILED',
        duration_sec      DOUBLE         COMMENT 'Duración en segundos de la ingesta',
        write_mode        STRING         COMMENT 'Modo de escritura: append | overwrite | merge | scd2',
        dedup_mode        STRING         COMMENT 'Estrategia de deduplicación',
        schema_version    INTEGER        COMMENT 'Versión del schema declarado en el JSON',
        source_filter     STRING         COMMENT 'Parámetro source_name recibido por el Job',
        notebook_path     STRING         COMMENT 'Path del notebook runner que generó el evento'
    )
    USING DELTA
    COMMENT 'Event log del pipeline de ingesta metadata-driven — FinPay'
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

print("✅ Tabla fintech_finpay.observability.pipeline_event_log lista")

# COMMAND ----------
# =============================================================================
# CELDA 3 — VERIFICAR
# =============================================================================

display(spark.sql("DESCRIBE TABLE fintech_finpay.observability.pipeline_event_log"))