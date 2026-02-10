{{ config(
    materialized='table'
) }}

with patient_data as (
    select * from {{ ref('fct_patient_risk_assessment') }}
),

risk_factor_impact as (
    select
        'smoking' as risk_factor,
        severity_level,
        count(patient_id) as patient_count,
        round(avg(smoking), 2) as avg_score,
        round(stddev(smoking), 2) as score_stddev,
        sum(case when smoking >= 6 then 1 else 0 end) as high_score_count

    from patient_data
    group by severity_level

    union all

    select
        'alcohol_use' as risk_factor,
        severity_level,
        count(patient_id) as patient_count,
        round(avg(alcohol_use), 2) as avg_score,
        round(stddev(alcohol_use), 2) as score_stddev,
        sum(case when alcohol_use >= 6 then 1 else 0 end) as high_score_count

    from patient_data
    group by severity_level

    union all

    select
        'obesity' as risk_factor,
        severity_level,
        count(patient_id) as patient_count,
        round(avg(obesity), 2) as avg_score,
        round(stddev(obesity), 2) as score_stddev,
        sum(case when obesity >= 6 then 1 else 0 end) as high_score_count

    from patient_data
    group by severity_level

    union all

    select
        'genetic_risk' as risk_factor,
        severity_level,
        count(patient_id) as patient_count,
        round(avg(genetic_risk), 2) as avg_score,
        round(stddev(genetic_risk), 2) as score_stddev,
        sum(case when genetic_risk >= 6 then 1 else 0 end) as high_score_count

    from patient_data
    group by severity_level

    union all

    select
        'air_pollution' as risk_factor,
        severity_level,
        count(patient_id) as patient_count,
        round(avg(air_pollution), 2) as avg_score,
        round(stddev(air_pollution), 2) as score_stddev,
        sum(case when air_pollution >= 6 then 1 else 0 end) as high_score_count

    from patient_data
    group by severity_level

    union all

    select
        'occupational_hazards' as risk_factor,
        severity_level,
        count(patient_id) as patient_count,
        round(avg(occupational_hazards), 2) as avg_score,
        round(stddev(occupational_hazards), 2) as score_stddev,
        sum(case when occupational_hazards >= 6 then 1 else 0 end) as high_score_count

    from patient_data
    group by severity_level

    union all

    select
        'passive_smoker' as risk_factor,
        severity_level,
        count(patient_id) as patient_count,
        round(avg(passive_smoker), 2) as avg_score,
        round(stddev(passive_smoker), 2) as score_stddev,
        sum(case when passive_smoker >= 6 then 1 else 0 end) as high_score_count

    from patient_data
    group by severity_level

    union all

    select
        'chronic_lung_disease' as risk_factor,
        severity_level,
        count(patient_id) as patient_count,
        round(avg(chronic_lung_disease), 2) as avg_score,
        round(stddev(chronic_lung_disease), 2) as score_stddev,
        sum(case when chronic_lung_disease >= 6 then 1 else 0 end) as high_score_count

    from patient_data
    group by severity_level
),

risk_factor_correlation as (
    select
        risk_factor,

        -- Average scores by severity
        max(case when severity_level = 'Low' then avg_score else null end) as low_severity_avg,
        max(case when severity_level = 'Medium' then avg_score else null end) as medium_severity_avg,
        max(case when severity_level = 'High' then avg_score else null end) as high_severity_avg,

        -- Calculate correlation strength (difference between high and low severity)
        max(case when severity_level = 'High' then avg_score else 0 end) -
        max(case when severity_level = 'Low' then avg_score else 0 end) as correlation_strength,

        -- Percentage of high scores in high severity patients
        round(100.0 * max(case when severity_level = 'High' then high_score_count else 0 end) /
              nullif(max(case when severity_level = 'High' then patient_count else 0 end), 0), 2) as high_severity_high_score_pct

    from risk_factor_impact
    group by risk_factor
),

risk_combinations as (
    select
        severity_level,

        -- Common risk factor combinations
        sum(case when smoking >= 6 and alcohol_use >= 6 then 1 else 0 end) as smoking_alcohol_combo,
        sum(case when smoking >= 6 and genetic_risk >= 6 then 1 else 0 end) as smoking_genetic_combo,
        sum(case when obesity >= 6 and chronic_lung_disease >= 6 then 1 else 0 end) as obesity_lung_combo,
        sum(case when air_pollution >= 6 and occupational_hazards >= 6 then 1 else 0 end) as environmental_combo,

        count(patient_id) as total_patients

    from patient_data
    group by severity_level
)

select * from risk_factor_correlation
order by correlation_strength desc