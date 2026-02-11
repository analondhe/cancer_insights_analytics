{{ config(
    materialized='table'
) }}

with patients as (
    select * from {{ ref('int_patient_lifestyle') }}
),

severity_counts as (
    select
        age_group,
        count(*) as total_patients,
        sum(case when severity_level = 'High' then 1 else 0 end) as high_severity_count,
        sum(case when severity_level = 'Medium' then 1 else 0 end) as medium_severity_count,
        sum(case when severity_level = 'Low' then 1 else 0 end) as low_severity_count
    from patients
    group by age_group
)

select
    age_group,
    total_patients,
    high_severity_count,
    medium_severity_count,
    low_severity_count,
    round(100.0 * high_severity_count / total_patients, 2) as high_severity_pct,
    round(100.0 * medium_severity_count / total_patients, 2) as medium_severity_pct,
    round(100.0 * low_severity_count / total_patients, 2) as low_severity_pct
from severity_counts
order by age_group
