# FinPay LakeHouse

Proyecto de arquitectura Lakehouse orientado al procesamiento, almacenamiento y análisis de datos utilizando un enfoque moderno basado en capas (`Bronze`, `Silver`, `Gold`) con la plataforma **Databricks**.

## Descripción

Este repositorio implementa una solución de ingeniería de datos para la empresa ficticia **FinPay**, permitiendo:

* Ingesta de datos desde múltiples fuentes.
* Procesamiento incremental y transformación de datos.
* Gobierno y trazabilidad de información.
* Modelado analítico para consumo de negocio.
* Automatización de pipelines de datos.
* Escalabilidad mediante arquitectura Lakehouse.

---

# Arquitectura

El proyecto sigue el patrón de arquitectura medallion:

## Capas

### Bronze

Contiene datos de las fuentes originales sin transformaciones.

Características:

* Se procesa con notebook tradicionales
* Las fuentes a cargar se declaran en el archivo:
```bash
bundle_dev_finpay/resources/ingestion_archetypes.json
```

* Datos históricos completos.
* Trazabilidad de origen.
* Validaciones mínimas.
* Persistencia raw.
* Para la fuente users, se creo un notebook para recuperar los registros que tienen 9 columnas:
```bash
bundle_dev_finpay/src/repo_bad_users.ipynb
```

### Silver

Contiene datos depurados y estandarizados.

Características:

* Se procesa con notebook tradicionales (fuente: transactions)
* Se procesa con declative pipeline (fuente usuarios y merchants)
* Limpieza de datos.
* Validaciones de calidad.
* Normalización de estructuras.
* Deduplicación.

### Gold

Contiene datasets listos para consumo analítico y reporting.

Características:

* Se procesa con declative pipeline 
* KPIs de negocio.
* Agregaciones.
* Tablas analíticas.
* Data marts.
* Consumo Power BI.

---
# Observabilidad

Tablero de Observabilidad

```bash
bundle_dev_finpay/dashboards/fintech_finpay.observability.pipeline_event_log.pbix
```

# Autor

## Jean Altamirano

Data Engineer | Analytics Engineer | BI Developer

Links:

* [github](https://github.com/jz3lada/)

* [linkedin](https://www.linkedin.com/in/jeanzelada/)
---
