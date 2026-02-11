{{ config(
    materialized='table'
) }}

with lifestyle_unpivot as (
    unpivot {{ ref('int_patient_lifestyle') }}
    on balanced_diet, alcohol_use, obesity, smoking, snoring
    into name factor_name value score
),

non_lifestyle_unpivot as (
    unpivot {{ ref('int_patient_non_lifestyle') }}
    on air_pollution, dust_allergy, occupational_hazards, genetic_risk, chronic_lung_disease, passive_smoker
    into name factor_name value score
),

all_factors as (
    select factor_name, 'lifestyle' as factor_type, severity_level, score from lifestyle_unpivot
    union all
    select factor_name, 'non_lifestyle' as factor_type, severity_level, score from non_lifestyle_unpivot
),

factor_averages as (
    select
        factor_name,
        factor_type,
        round(avg(case when severity_level = 'Low' then score end), 2) as avg_score_low_severity,
        round(avg(case when severity_level = 'Medium' then score end), 2) as avg_score_medium_severity,
        round(avg(case when severity_level = 'High' then score end), 2) as avg_score_high_severity
    from all_factors
    group by factor_name, factor_type
)

select
    factor_name,
    factor_type,
    avg_score_low_severity,
    avg_score_medium_severity,
    avg_score_high_severity,
    round(avg_score_high_severity - avg_score_low_severity, 2) as high_vs_low_difference,
    rank() over (order by (avg_score_high_severity - avg_score_low_severity) desc) as impact_rank
from factor_averages
order by impact_rank
