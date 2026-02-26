# E-commerce DWH Medallion Project

Et end-to-end **Data Warehouse-projekt** bygget med en klassisk **Medallion-arkitektur**:

- **Bronze**: rå data ingest (uændret struktur)
- **Silver**: rensning, type-casting, validering og relationel integritet
- **Gold**: Kimball-inspireret stjerneskema + business views til BI/analytics

Projektet viser et realistisk fundament for analytiske workloads: schema-separation, idempotente loads, quality gates og forbrugsvenlige datasæt.

---

## Indhold

1. [Formål](#formål)
2. [Arkitektur](#arkitektur)
3. [Projektstruktur](#projektstruktur)
4. [Datamodel (Gold)](#datamodel-gold)
5. [Data Quality strategi](#data-quality-strategi)
6. [Kørsel (step-by-step)](#kørsel-step-by-step)
7. [Nøglekompetencer demonstreret](#nøglekompetencer-demonstreret)
8. [Forbedringsmuligheder (næste iteration)](#forbedringsmuligheder-næste-iteration)
9. [CV/Portfolio-tekst (copy/paste)](#cvportfolio-tekst-copypaste)

---

## Formål

Målet med projektet er at bygge et robust analytisk datagrundlag fra bunden, hvor data bevæger sig gennem modenhedsniveauer:

- fra rå, sporbar ingestion,
- til valideret og relationelt konsistent data,
- til KPI-klar model for dashboards, rapportering og ad-hoc analyse.

---

## Arkitektur

### Bronze (Raw Zone)

- Data beholdes så tæt på kilden som muligt.
- Fokus: sporbarhed, replay/idempotens, minimal transformation.
- Egnet til reprocessing og fejlfinding.

### Silver (Clean Zone)

- Datatyper rettes og kolonner standardiseres.
- Primærnøgler/unikhed håndhæves.
- Referentiel integritet kontrolleres (fx orphan checks).
- Domæneregler og sanity checks fungerer som quality gate.

### Gold (Business Zone)

- Kimball-inspireret dimensional model.
- Faktatabel + dimensionstabeller (star schema).
- Business-facing views skjuler teknisk kompleksitet (surrogate keys).
- Optimeret for BI-værktøjer og analytiske forespørgsler.

---

## Projektstruktur

```text
.
├── docs/
│   ├── bronze_layer_theory.md
│   ├── silver_layer_theory.md
│   ├── gold_layer_theory.md
│   ├── naming_conventions.md
│   └── ERD-diagram.png
└── sql-scripts/
    ├── setup_database_infrastructure/
    │   └── 01_create_database_and_schemas.sql
    ├── bronze/
    │   ├── 01_create_bronze_tables.sql
    │   ├── 02_load_bronze.sql
    │   └── 03_bronze_checks.sql
    ├── silver/
    │   ├── 01_create_silver_tables.sql
    │   ├── 02_load_silver.sql
    │   └── 03_silver_quality_checks.sql
    └── gold/
        ├── 01_create_gold_tables.sql
        ├── 02_load_gold.sql
        └── 03_views.sql
```

---

## Datamodel (Gold)

Gold-laget er designet til analytics med:

- **Dimensioner** (fx kunder, produkter, sælgere, dato)
- **Fakta** på ordreniveau/orderline-grain
- **Forbrugsviews** til standard KPI’er og downstream BI

Typiske analyser:

- omsætning over tid
- salg pr. produktkategori
- kundeperformance og review-score
- shipping- og leveringsrelaterede trends

---

## Data Quality strategi

Projektet arbejder med quality gates før data må gå videre i pipelinen:

- **Bronze checks**: basale row count sanity checks
- **Silver checks**:
  - tabel-eksistens
  - null violations på NOT NULL-felter
  - PK/unikhedsforventninger
  - orphan checks mellem parent/child-tabeller
  - fail-fast via fejlstop ved kritiske brud

Denne strategi gør modellen mere driftssikker og reducerer risiko for “stille databrud”.

---

## Kørsel (step-by-step)

> Kør scripts i denne rækkefølge.

1. **Setup database + schemas**
   - `sql-scripts/setup_database_infrastructure/01_create_database_and_schemas.sql`
2. **Bronze**
   - `sql-scripts/bronze/01_create_bronze_tables.sql`
   - `sql-scripts/bronze/02_load_bronze.sql`
   - `sql-scripts/bronze/03_bronze_checks.sql`
3. **Silver**
   - `sql-scripts/silver/01_create_silver_tables.sql`
   - `sql-scripts/silver/02_load_silver.sql`
   - `sql-scripts/silver/03_silver_quality_checks.sql`
4. **Gold**
   - `sql-scripts/gold/01_create_gold_tables.sql`
   - `sql-scripts/gold/02_load_gold.sql`
   - `sql-scripts/gold/03_views.sql`

---

## Nøglekompetencer demonstreret

- SQL data modeling (OLAP/Kimball mindset)
- Medallion arkitektur i praksis
- Datakvalitet og fail-fast quality gates
- Relationel integritet og nøgledesign
- Idempotente load-mønstre
- Forretningsrettet dataleverance via views

---

## Forbedringsmuligheder (næste iteration)

### 1) Orkestrering og drift

- Flyt kørselsflow til en scheduler (fx Databricks Jobs / Airflow / ADF).
- Implementér miljøer (dev/test/prod) med parametriserede pipelines.
- Tilføj retry-strategi, alerting og run metadata.

### 2) Incremental loading

- Introducér watermark/CDC-baserede loads i Silver/Gold.
- Bevar full reload i Bronze ved behov for replay.
- Reducér runtime og compute-omkostning i takt med datavækst.

### 3) DQ-observability

- Skriv DQ-resultater til audit-tabeller.
- Definér datakontrakter + threshold-baserede regler.
- Synliggør datakvalitet i dashboard (pass/fail trends).

### 4) Historik (SCD Type 2)

- Udvid udvalgte dimensioner til Type 2, hvor historik er forretningskritisk.
- Tilføj `valid_from`, `valid_to`, `is_current`.
- Muliggør tidskorrekt historisk analyse på attributændringer.

### 5) Performance og skalerbarhed

- Benchmark top-queries fra BI og optimer indeksstrategi.
- Overvej partitionering på faktatabeller ved større datamængder.
- Tilføj materialiserede lag/aggregater for de tungeste KPI’er.

### 6) Governance og CI/CD

- Automatisér SQL-validering i CI.
- Versionér migrations mere formelt.
- Indfør PR-checkliste for schema drift + data quality compliance.

### 7) Moderne data platform (Databricks-spor)

Forslag til næste repo:

- API-ingestion (REST/JSON) → Bronze (raw)
- Structured transformations i Silver (Spark/Delta)
- Gold marts + job orchestration
- Fokus på job dependencies, retries, monitoring og incremental loads

Dette bygger direkte videre på kompetencerne i dette repo og viser platform-agnostisk modenhed.
