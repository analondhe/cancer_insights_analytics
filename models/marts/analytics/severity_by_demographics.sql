{{ config(
    materialized='table'
) }}

with patient_data as (
    select * from {{ ref('fct_patient_risk_assessment') }}
),

severity_by_age_gender as (
    select
        age_group,
        gender,
        severity_level,
        count(patient_id) as patient_count,
        round(avg(age), 2) as avg_age,
        round(avg(avg_risk_score), 2) as avg_risk_score,
        round(avg(avg_symptom_severity), 2) as avg_symptom_severity

    from patient_data
    group by age_group, gender, severity_level
),

severity_distribution as (
    select
        age_group,
        gender,
        sum(case when severity_level = 'Low' then patient_count else 0 end) as low_severity_count,
        sum(case when severity_level = 'Medium' then patient_count else 0 end) as medium_severity_count,
        sum(case when severity_level = 'High' then patient_count else 0 end) as high_severity_count,
        sum(patient_count) as total_patients,

        -- Percentages
        round(100.0 * sum(case when severity_level = 'Low' then patient_count else 0 end) / nullif(sum(patient_count), 0), 2) as low_severity_pct,
        round(100.0 * sum(case when severity_level = 'Medium' then patient_count else 0 end) / nullif(sum(patient_count), 0), 2) as medium_severity_pct,
        round(100.0 * sum(case when severity_level = 'High' then patient_count else 0 end) / nullif(sum(patient_count), 0), 2) as high_severity_pct

    from severity_by_age_gender
    group by age_group, gender
),

age_range_analysis as (
    select
        age_range,
        severity_level,
        count(patient_id) as patient_count,
        round(avg(avg_risk_score), 2) as avg_risk_score,
        round(avg(avg_symptom_severity), 2) as avg_symptom_severity,
        round(avg(high_risk_factor_count), 2) as avg_high_risk_factors

    from patient_data
    group by age_range, severity_level
),

final_output as (
    select
        'age_gender_distribution' as analysis_type,
        age_group,
        gender,
        null as age_range,
        null as severity_level,
        total_patients,
        low_severity_count,
        medium_severity_count,
        high_severity_count,
        low_severity_pct,
        medium_severity_pct,
        high_severity_pct,
        null as avg_risk_score,
        null as avg_symptom_severity,
        null as avg_high_risk_factors

    from severity_distribution

    union all

    select
        'age_range_severity' as analysis_type,
        null as age_group,
        null as gender,
        age_range,
        severity_level,
        patient_count as total_patients,
        null as low_severity_count,
        null as medium_severity_count,
        null as high_severity_count,
        null as low_severity_pct,
        null as medium_severity_pct,
        null as high_severity_pct,
        avg_risk_score,
        avg_symptom_severity,
        avg_high_risk_factors

    from age_range_analysis
)

select * from final_output
order by analysis_type, age_group, gender, age_range, severity_level