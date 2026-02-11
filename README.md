# Cancer Insights Analytics - dbt Project

A comprehensive dbt project for analyzing cancer patient data using DuckDB as the data warehouse.
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

## Data Models

### Staging Layer
- **stg_patients**: Cleans raw patient data, standardizes column names, converts gender codes to text

### Intermediate Layer
- **int_patient_risk_profile**: Categorizes risk factors (smoking, alcohol, obesity, etc.) as Low/Medium/High
- **int_patient_symptoms**: Aggregates symptom severity and identifies critical symptoms

### Marts Layer

#### Core Models
- **dim_patients**: Patient dimension with age groups and demographics
- **fct_patient_risk_assessment**: Comprehensive fact table with all patient metrics

#### Analytics Models
- **severity_by_demographics**: Analyzes cancer severity by age and gender
- **risk_factor_analysis**: Identifies which risk factors correlate with high severity
- **symptom_correlation**: Analyzes which symptoms appear most in high severity cases

## Macros

- **age_bucket**: Converts age to age groups (Young/Middle-aged/Senior/Elderly)
- **risk_level**: Converts 1-8 scale to Low/Medium/High categories

## Key Insights Available

1. **Demographic Analysis**:
   - Cancer severity distribution by age groups and gender
   - High-risk age demographics

2. **Risk Factor Analysis**:
   - Which risk factors have strongest correlation with high severity
   - Common risk factor combinations in severe cases

3. **Symptom Analysis**:
   - Critical symptoms in high severity patients
   - Symptom clustering patterns

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
