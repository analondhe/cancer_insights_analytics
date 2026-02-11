# Cancer Insights Analytics - dbt Project

A comprehensive dbt project for analyzing cancer patient data using DuckDB as the data warehouse.# Cancer Insights Analytics

A dbt project analyzing cancer patient data to identify severity patterns and rank risk factors.

The transformed dataset address:
- Which demographics have higher cancer severity rates?
- What factors correlate most with cancer severity?
<img width="1982" height="392" alt="Screenshot 2026-02-10 at 8 29 53 PM" src="https://github.com/user-attachments/assets/25cece0e-ce0c-461d-9f47-cf37aa40a9be" />

## Project Structure

```
cancer_insights_analytics/
├── models/
│   ├── staging/           # Raw data cleaning and standardization
│   ├── intermediate/      # Business logic transformations
│   └── marts/             # Business-ready datasets
│       ├── core/          # Dimension and fact tables
│       └── analytics/     # Analysis-specific models
├── macros/                # Reusable SQL functions
├── seeds/                 # CSV data files
├── tests/                 # Custom tests
├── analyses/              # Ad-hoc analyses
└── snapshots/            # SCD Type 2 history tracking
```

## Setup Instructions

1. **Install dbt-duckdb**:
   ```bash
   pip install dbt-duckdb
   ```

2. **Place your data file**:
   - Copy `cancer_patient_data_sets.csv` to the `seeds/` folder

3. **Load the seed data**:
   ```bash
   dbt seed
   ```

4. **Run the models**:
   ```bash
   dbt run
   ```

5. **Test the models**:
   ```bash
   dbt test
   ```



## Data Source

- **Source:** [Kaggle - Cancer Patients and Air Pollution](https://www.kaggle.com/datasets/thedevastator/cancer-patients-and-air-pollution-a-new-link)
- **Records:** 1,000 patients
- **Features:** Demographics, lifestyle factors, environmental factors, severity level

## Project Structure
```
├── seeds/
│   └── cancer_patient_data_sets.csv
├── models/
│   ├── staging/
│   │   └── stg_patients.sql
│   ├── intermediate/
│   │   ├── int_patient_lifestyle.sql
│   │   └── int_patient_non_lifestyle.sql
│   └── marts/
│       ├── severity_by_gender.sql
│       ├── severity_by_age_group.sql
│       └── risk_factor_analysis.sql
└── macros/
    └── age_bucket.sql
```

## Data Flow
```
seed → staging → intermediate → marts
                      │
         ┌───────────┴───────────┐
         │                       │
   int_patient            int_patient
   _lifestyle             _non_lifestyle
         │                       │
         └───────────┬───────────┘
                     │
    ┌────────────────┼────────────────┐
    │                │                │
severity_by    severity_by     risk_factor
_gender        _age_group      _analysis
```

## Layer Descriptions

| Layer | Purpose |
|-------|---------|
| **Staging** | Clean and rename columns |
| **Intermediate** | Split into lifestyle vs non-lifestyle factors |
| **Marts** | Business-ready aggregations and rankings |

## Mart Models

| Model | Description |
|-------|-------------|
| `severity_by_gender` | Severity distribution by gender |
| `severity_by_age_group` | Severity distribution by age group |
| `risk_factor_analysis` | Ranks factors by correlation with severity |

## Key Findings

- Alcohol use has the strongest correlation with severity
- Obesity ranks second
- Gender 1 has 42% high severity vs Gender 2 at 28%
- Severity increases with age

## Setup
```bash
# Install dbt with DuckDB
pip install dbt-duckdb

# Run the project
dbt seed
dbt run
dbt test

# Generate docs
dbt docs generate
dbt docs serve
```

## Tech Stack

- **Transformation:** dbt Core
- **Warehouse:** DuckDB
- **Language:** SQL + Jinja
## Configuration

The project uses DuckDB with the following settings:
- Database: `cancer_insights.duckdb` (created automatically)
- Schema separation for each layer (staging, intermediate, marts)
- Tables materialized for marts, views for staging/intermediate

## Running Queries

After running the models, you can query the data:

```sql
-- Example: Top risk factors for high severity
SELECT * FROM marts_analytics.risk_factor_analysis
ORDER BY correlation_strength DESC;

-- Example: Severity by age and gender
SELECT * FROM marts_analytics.severity_by_demographics
WHERE analysis_type = 'age_gender_distribution';
```
