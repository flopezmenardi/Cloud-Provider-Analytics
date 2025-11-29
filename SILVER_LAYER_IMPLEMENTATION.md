# Silver Layer Implementation Summary

## ✅ Completado

### Framework de Calidad de Datos
**Archivo:** `src/common/data_quality.py`

**Funciones implementadas:**
- `validate_ranges()` - Validar rangos de valores (ej: cost >= -0.01)
- `detect_outliers_zscore()` - Detección usando Z-score (threshold=3.0)
- `detect_outliers_mad()` - Detección usando MAD (Median Absolute Deviation)
- `detect_outliers_percentile()` - Detección usando percentiles (default: 1%-99%)
- `add_anomaly_flags()` - Añade flags de los 3 métodos + flag combinado
- `quarantine_records()` - Separa registros válidos/inválidos y escribe quarantine
- `add_data_quality_metrics()` - Añade métricas de calidad (quality_score, is_valid, quality_issues)

---

### Transformaciones Silver - Batch Sources (7 archivos)

#### 1. `src/silver/transform_customers_orgs_silver.py`
**Transformaciones:**
- Estandarizar industry, region, tier, plan_type (lowercase, trim)
- Normalizar NPS scores (validar 0-10, null si fuera de rango)
- Validar org_id y org_name no nulos
- Añadir silver_processing_timestamp

**Output:** `datalake/silver/customers_orgs/`

#### 2. `src/silver/transform_users_silver.py`
**Transformaciones:**
- Limpiar y estandarizar email (lowercase, trim)
- Validar formato de email con regex
- Estandarizar role (lowercase, trim)
- Parsear last_login a timestamp (nullable)
- Validar active status como boolean
- Validar user_id, org_id, email no nulos

**Output:** `datalake/silver/users/`

#### 3. `src/silver/transform_resources_silver.py`
**Transformaciones:**
- Estandarizar service, region, resource_type, state (lowercase, trim)
- Validar service ∈ {compute, storage, database, networking, analytics, genai}
- Validar resource_id y org_id no nulos

**Output:** `datalake/silver/resources/`

#### 4. `src/silver/transform_billing_monthly_silver.py`
**Transformaciones:**
- Manejar NULL credits (default 0)
- Manejar NULL taxes (default 0)
- Convertir todos los montos a USD usando exchange_rate_to_usd
- Calcular net_amount = amount_usd - credits + taxes
- Estandarizar currency a "USD"
- Validar amount_usd >= 0
- Validar invoice_id y org_id no nulos

**Output:** `datalake/silver/billing_monthly/`

#### 5. `src/silver/transform_support_tickets_silver.py`
**Transformaciones:**
- Estandarizar status ∈ {open, in_progress, resolved, closed}
- Estandarizar severity ∈ {low, medium, high, critical}
- Estandarizar category (lowercase, trim)
- Parsear created_at y resolved_at a timestamps
- Calcular resolution_time_hours = (resolved_at - created_at) / 3600
- Validar CSAT score (1-5 si existe, null si fuera de rango)
- Validar ticket_id, org_id no nulos

**Output:** `datalake/silver/support_tickets/`

#### 6. `src/silver/transform_marketing_touches_silver.py`
**Transformaciones:**
- Estandarizar channel, touch_type, campaign_name (lowercase, trim)
- Parsear touch_timestamp a timestamp
- Convertir converted a boolean
- Validar touch_id y org_id no nulos

**Output:** `datalake/silver/marketing_touches/`

#### 7. `src/silver/transform_nps_surveys_silver.py`
**Transformaciones:**
- Parsear survey_date a timestamp
- Validar NPS score (0-10, null si fuera de rango)
- Validar survey_id, org_id, nps_score no nulos

**Output:** `datalake/silver/nps_surveys/`

---

### Transformación Silver - Streaming (CRÍTICO)

#### 8. `src/silver/transform_usage_events_silver.py`
**Archivo más importante del Silver Layer**

**Transformaciones principales:**

1. **Normalización de value column:**
   - Convierte value (string) → double con fallback
   - Maneja tipos mixtos string/numeric

2. **Compatibilización schema_version v1/v2:** ⭐
   - v1 (pre 2025-07-18): carbon_kg=null, genai_tokens=null
   - v2 (post 2025-07-18): usa valores reales de carbon_kg y genai_tokens
   - Crea columnas unificadas para ambas versiones

3. **Normalización de dimensiones:**
   - Estandarizar region, service, metric, unit (lowercase, trim)

4. **Validaciones (según PDF del proyecto):**
   - event_id no nulo (dedup ya en Bronze)
   - cost_usd_increment >= -0.01
   - unit no nulo cuando value no nulo (imputa "unknown" si falta)
   - schema_version válido (1 o 2)

5. **Enriquecimiento:**
   - Join con resources → resource_service, resource_type, state, resource_found flag
   - Join con customers_orgs → org_name, tier, plan_type, industry, org_found flag

6. **Campos derivados:**
   - event_date, event_year, event_month, event_day (para particionado)
   - silver_processing_timestamp

7. **Detección de anomalías (3 métodos):** ⭐
   - Z-score (threshold=3.0) → is_anomaly_zscore
   - MAD - Median Absolute Deviation (threshold=3.0) → is_anomaly_mad
   - Percentiles (1%-99%) → is_anomaly_percentile
   - Flag combinado → is_anomaly (TRUE si cualquier método detecta anomalía)

8. **Quarantine:**
   - Registros inválidos → `datalake/silver/quarantine/usage_events/`

**Output:** `datalake/silver/usage_events/` (particionado por event_year, event_month)

**Métricas reportadas:**
- Conteo inicial vs final
- Registros por schema_version (v1 vs v2)
- Anomalías detectadas (count y %)
- Registros válidos vs quarantine (count y %)
- Data quality rate

---

### Orquestador Silver

#### `src/silver/transform_all.py`
**Función:** Ejecuta todas las transformaciones Silver en orden de dependencia

**Orden de ejecución:**
1. customers_orgs (referenciado por otros)
2. resources (referenciado por usage_events)
3. users
4. billing_monthly
5. support_tickets
6. marketing_touches
7. nps_surveys
8. usage_events (último, usa enriquecimiento)

**Características:**
- Manejo de errores: continúa con otras transformaciones si una falla
- Reporte de métricas: duración, éxito/fallo de cada transformación
- Resumen final con estadísticas

**Ejecución:**
```bash
./scripts/run_silver_transformations.sh
```

---

## Configuración de AstraDB/Cassandra

### Archivos de configuración

#### `src/config/cassandra_config.py`
**Configuración para conectar a AstraDB:**
- Secure connect bundle path
- Client ID y Client Secret (application token)
- Keyspace: `cloud_analytics`
- Modo: "astradb" o "local"
- Función de validación de config

**Variables de entorno soportadas:**
- `CASSANDRA_MODE` (default: "astradb")
- `ASTRADB_SECURE_CONNECT_BUNDLE`
- `ASTRADB_CLIENT_ID`
- `ASTRADB_CLIENT_SECRET`

#### `src/serving/cassandra_setup.py`
**Script de setup para crear keyspace y tablas**

**5 Tablas creadas (query-first design):**

1. **org_daily_usage** - Para Query 1
   - Partition key: org_id
   - Clustering: usage_date DESC, service ASC
   - Columnas: total_cost_usd, total_requests, cpu_hours, storage_gb_hours, carbon_kg, genai_tokens

2. **org_service_costs** - Para Query 2
   - Partition key: (org_id, window_days)
   - Clustering: total_cost_usd DESC, service ASC
   - Para Top-N servicios por costo

3. **tickets_critical_daily** - Para Query 3
   - Partition key: date
   - Clustering: severity ASC
   - Columnas: total_tickets, open_tickets, resolved_tickets, sla_breach_rate, avg_resolution_hours

4. **revenue_monthly** - Para Query 4
   - Partition key: org_id
   - Clustering: year_month DESC
   - Columnas: org_name, total_billed_usd, total_credits, total_taxes, net_revenue, invoice_count

5. **genai_tokens_daily** - Para Query 5
   - Partition key: org_id
   - Clustering: usage_date DESC
   - Columnas: total_genai_tokens, total_cost_usd, cost_per_million_tokens

**Ejecución:**
```bash
python -m src.serving.cassandra_setup
```

---

## Actualizaciones de Configuración

### `src/config/paths.py`
**Añadido:**
- `SILVER_ROOT` - Directorio raíz Silver
- `SILVER_QUARANTINE` - Directorio para datos en quarantine
- `GOLD_ROOT` - Directorio raíz Gold
- `get_silver_path(source_name)` - Helper para paths Silver
- `get_quarantine_path(source_name)` - Helper para paths Quarantine
- `get_gold_path(mart_name)` - Helper para paths Gold
- Creación automática de directorios Silver y Gold

### `requirements.txt`
**Añadido:**
- `cassandra-driver>=3.28.0` - Para conectar a AstraDB/Cassandra

### `README.md`
**Actualizado con:**
- Instrucciones de Phase 2: Silver Layer
- Instrucciones de Phase 5: Serving Layer (AstraDB setup)

---

## Archivos Creados (Total: 15)

### Framework y Utilidades (2)
1. `src/common/data_quality.py`
2. `src/config/paths.py` (actualizado)

### Transformaciones Silver - Batch (7)
3. `src/silver/transform_customers_orgs_silver.py`
4. `src/silver/transform_users_silver.py`
5. `src/silver/transform_resources_silver.py`
6. `src/silver/transform_billing_monthly_silver.py`
7. `src/silver/transform_support_tickets_silver.py`
8. `src/silver/transform_marketing_touches_silver.py`
9. `src/silver/transform_nps_surveys_silver.py`

### Transformaciones Silver - Streaming (1)
10. `src/silver/transform_usage_events_silver.py` ⭐

### Orquestación (2)
11. `src/silver/transform_all.py`
12. `scripts/run_silver_transformations.sh`

### Serving / Cassandra (2)
13. `src/config/cassandra_config.py`
14. `src/serving/cassandra_setup.py`

### Otros (1)
15. `src/silver/__init__.py`
16. `src/serving/__init__.py`

---

## Próximos Pasos

### 1. Ejecutar Bronze Layer (si no está hecho)
```bash
./scripts/run_batch_ingestion.sh
./scripts/run_streaming_ingestion.sh batch
```

### 2. Ejecutar Silver Layer
```bash
./scripts/run_silver_transformations.sh
```

**Verificar:**
- Datos en `datalake/silver/` para cada fuente
- Datos en quarantine en `datalake/silver/quarantine/`
- Logs de anomalías detectadas
- Logs de compatibilidad v1/v2

### 3. Setup AstraDB
1. Crear cuenta en https://astra.datastax.com/
2. Crear database `cloud_analytics`
3. Descargar secure connect bundle
4. Crear application token
5. Actualizar `src/config/cassandra_config.py`
6. Ejecutar: `python -m src.serving.cassandra_setup`

### 4. Implementar Gold Layer (siguiente fase)
- Crear 5 marts de negocio según plan
- org_daily_usage_by_service
- revenue_by_org_month
- cost_anomaly_mart
- tickets_by_org_date
- genai_tokens_by_org_date

### 5. Cargar datos a Cassandra
- Implementar `src/serving/load_to_cassandra.py`
- Leer marts de Gold
- Insertar en tablas Cassandra

### 6. Implementar queries de demo
- Implementar `src/serving/demo_queries.py`
- 5 consultas CQL según requisitos del PDF

---

## Características Destacadas

### ✅ Compatibilidad Schema v1/v2
Maneja automáticamente la evolución de esquema en usage_events:
- Detecta schema_version
- Crea columnas unificadas (carbon_kg, genai_tokens)
- v1 → null, v2 → valores reales
- Reporta conteo de registros por versión

### ✅ Detección de Anomalías (3 métodos)
Según requisitos del PDF del proyecto:
- Z-score (estadístico, asume distribución normal)
- MAD (robusto a valores extremos)
- Percentiles (no-paramétrico)
- Flag combinado para cualquier método

### ✅ Quarantine de Datos Malos
- Separa registros válidos/inválidos
- Escribe quarantine a Parquet
- Reporta % de calidad
- Permite depuración sin bloquear pipeline

### ✅ Enriquecimiento con Joins
- usage_events + resources → metadata de recursos
- usage_events + customers_orgs → tier/plan/industry
- Flags de encontrado (resource_found, org_found)

### ✅ Idempotencia
- Modo overwrite en todas las escrituras
- Permite re-ejecución sin duplicar datos

### ✅ Logging Detallado
- Conteos inicial/final
- Métricas de calidad
- Duración de cada paso
- Resumen de éxito/fallo

---

## Decisiones de Diseño

### Por qué Silver primero, luego Gold, luego Cassandra
1. **Silver** limpia y estandariza datos (fundación)
2. **Gold** crea marts agregados desde Silver limpio
3. **Cassandra** carga desde Gold (datos ya procesados)

### Por qué AstraDB vs Cassandra Local
- Free tier suficiente para proyecto
- No requiere Docker ni instalación local
- Setup en minutos vs horas
- Producción-ready desde el inicio

### Por qué 3 métodos de detección de anomalías
Requisito del PDF del proyecto. Cada método tiene fortalezas:
- **Z-score:** rápido, estadístico estándar
- **MAD:** robusto a outliers extremos
- **Percentiles:** no asume distribución

### Por qué orden de transformaciones
Dependencias entre datasets:
- `customers_orgs` primero → referenciado por users, usage_events
- `resources` segundo → referenciado por usage_events
- `usage_events` último → usa enriquecimiento de ambos

---

## Validación

Para verificar que todo funciona correctamente:

```bash
# 1. Verificar que Silver se creó correctamente
ls -lh datalake/silver/
ls -lh datalake/silver/quarantine/

# 2. Contar registros en Silver (ejemplo con PySpark)
pyspark
>>> df = spark.read.parquet("datalake/silver/usage_events")
>>> df.count()
>>> df.groupBy("schema_version").count().show()
>>> df.filter("is_anomaly = true").count()

# 3. Verificar configuración de Cassandra
python -m src.config.cassandra_config

# 4. Verificar tablas Cassandra (después de setup)
python -m src.serving.cassandra_setup
```

---

## Troubleshooting

### Error: "Secure connect bundle not found"
- Descargar bundle desde AstraDB Dashboard
- Actualizar path en `cassandra_config.py`
- O establecer variable: `export ASTRADB_SECURE_CONNECT_BUNDLE=/path/to/bundle.zip`

### Error: "Client ID not configured"
- Crear application token en AstraDB
- Actualizar en `cassandra_config.py`
- O usar variables de entorno

### Error: "Keyspace cloud_analytics does not exist"
- Para AstraDB: crear keyspace via web UI
- Para local: el script lo crea automáticamente

### Pocos registros en Silver vs Bronze
- Normal: quarantine separa registros inválidos
- Revisar logs para ver razones de quarantine
- Revisar `datalake/silver/quarantine/` para datos rechazados
