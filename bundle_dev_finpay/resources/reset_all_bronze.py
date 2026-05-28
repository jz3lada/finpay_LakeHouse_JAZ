# ============================================================
# NOTEBOOK: reset_all_bronze.py
# OBJETIVO:
# Resetear completamente las tablas Bronze y el estado
# de Auto Loader para forzar reprocesamiento FULL.
#
# Este notebook:
#   1. Trunca tablas Bronze
#   2. Elimina checkpoints
#   3. Elimina schema tracking
#   4. (Opcional) elimina bad records
#
# IMPORTANTE:
# SOLO usar en DEV / QA.
# NO ejecutar indiscriminadamente en PROD.
# ============================================================

from pyspark.sql import functions as F

# ============================================================
# CONFIGURACION
# ============================================================

CATALOG_NAME = "fintech_finpay"
SCHEMA_NAME = "bronze"

SOURCES = [
    "users",
    "transactions",
    "merchants"
]

DELETE_BAD_RECORDS = True

# ============================================================
# FUNCIONES
# ============================================================

def truncate_table(source_name: str):

    table_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{source_name}"

    print("=" * 80)
    print(f"[TRUNCATE] → {table_name}")

    spark.sql(f"TRUNCATE TABLE {table_name}")

    print(f"[OK] Tabla truncada")


def delete_checkpoint(source_name: str):

    path = f"/Volumes/fintech_finpay/default/vol_metadata/checkpoints/{source_name}/"

    print(f"[DELETE CHECKPOINT] → {path}")

    dbutils.fs.rm(path, recurse=True)

    print(f"[OK] Checkpoint eliminado")


def delete_schema_tracking(source_name: str):

    path = f"/Volumes/fintech_finpay/default/vol_metadata/schemas/{source_name}/"

    print(f"[DELETE SCHEMA TRACKING] → {path}")

    dbutils.fs.rm(path, recurse=True)

    print(f"[OK] Schema tracking eliminado")


def delete_bad_records(source_name: str):

    path = f"/Volumes/fintech_finpay/default/vol_metadata/bad_records/{source_name}/"

    print(f"[DELETE BAD RECORDS] → {path}")

    dbutils.fs.rm(path, recurse=True)

    print(f"[OK] Bad records eliminados")

# ============================================================
# EJECUCION PRINCIPAL
# ============================================================

print()
print("=" * 80)
print("INICIANDO RESET COMPLETO DE BRONZE")
print("=" * 80)
print()

for source in SOURCES:

    try:

        print()
        print("#" * 80)
        print(f"PROCESANDO FUENTE: {source}")
        print("#" * 80)
        print()

        # ----------------------------------------------------
        # 1. TRUNCATE TABLA BRONZE
        # ----------------------------------------------------

        truncate_table(source)

        # ----------------------------------------------------
        # 2. ELIMINAR CHECKPOINT
        # ----------------------------------------------------

        delete_checkpoint(source)

        # ----------------------------------------------------
        # 3. ELIMINAR SCHEMA TRACKING
        # ----------------------------------------------------

        delete_schema_tracking(source)

        # ----------------------------------------------------
        # 4. ELIMINAR BAD RECORDS (OPCIONAL)
        # ----------------------------------------------------

        if DELETE_BAD_RECORDS:
            delete_bad_records(source)

        print()
        print(f"[SUCCESS] Reset completado para fuente: {source}")

    except Exception as e:

        print()
        print(f"[ERROR] Fuente: {source}")
        print(str(e))

print()
print("=" * 80)
print("RESET BRONZE FINALIZADO")
print("=" * 80)
print()

# ============================================================
# VALIDACION FINAL
# ============================================================

print()
print("=" * 80)
print("VALIDACION FINAL DE TABLAS")
print("=" * 80)
print()

for source in SOURCES:

    table_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{source}"

    print(f"[VALIDATE] → {table_name}")

    display(
        spark.sql(
            f"""
            SELECT
                '{source}' AS source_name,
                COUNT(*) AS total_records
            FROM {table_name}
            """
        )
    )