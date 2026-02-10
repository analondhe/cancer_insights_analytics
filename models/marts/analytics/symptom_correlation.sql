{{ config(
    materialized='table'
) }}

with patient_data as (
    select * from {{ ref('fct_patient_risk_assessment') }}
),

symptom_severity_analysis as (
    select
        'chest_pain' as symptom,
        severity_level,
        count(patient_id) as patient_count,
        round(avg(chest_pain), 2) as avg_score,
        sum(case when chest_pain >= 6 then 1 else 0 end) as high_score_count

    from patient_data
    group by severity_level

    union all

    select
        'coughing_blood' as symptom,
        severity_level,
        count(patient_id) as patient_count,
        round(avg(coughing_blood), 2) as avg_score,
        sum(case when coughing_blood >= 6 then 1 else 0 end) as high_score_count

    from patient_data
    group by severity_level

    union all

    select
        'fatigue' as symptom,
        severity_level,
        count(patient_id) as patient_count,
        round(avg(fatigue), 2) as avg_score,
        sum(case when fatigue >= 6 then 1 else 0 end) as high_score_count

    from patient_data
    group by severity_level

    union all

    select
        'weight_loss' as symptom,
        severity_level,
        count(patient_id) as patient_count,
        round(avg(weight_loss), 2) as avg_score,
        sum(case when weight_loss >= 6 then 1 else 0 end) as high_score_count

    from patient_data
    group by severity_level

    union all

    select
        'shortness_breath' as symptom,
        severity_level,
        count(patient_id) as patient_count,
        round(avg(shortness_breath), 2) as avg_score,
        sum(case when shortness_breath >= 6 then 1 else 0 end) as high_score_count

    from patient_data
    group by severity_level

    union all

    select
        'wheezing' as symptom,
        severity_level,
        count(patient_id) as patient_count,
        round(avg(wheezing), 2) as avg_score,
        sum(case when wheezing >= 6 then 1 else 0 end) as high_score_count

    from patient_data
    group by severity_level

    union all

    select
        'swallowing_difficulty' as symptom,
        severity_level,
        count(patient_id) as patient_count,
        round(avg(swallowing_difficulty), 2) as avg_score,
        sum(case when swallowing_difficulty >= 6 then 1 else 0 end) as high_score_count

    from patient_data
    group by severity_level

    union all

    select
        'clubbing_fingernails' as symptom,
        severity_level,
        count(patient_id) as patient_count,
        round(avg(clubbing_fingernails), 2) as avg_score,
        sum(case when clubbing_fingernails >= 6 then 1 else 0 end) as high_score_count

    from patient_data
    group by severity_level
),

symptom_correlation as (
    select
        symptom,

        -- Average scores by severity
        max(case when severity_level = 'Low' then avg_score else null end) as low_severity_avg,
        max(case when severity_level = 'Medium' then avg_score else null end) as medium_severity_avg,
        max(case when severity_level = 'High' then avg_score else null end) as high_severity_avg,

        -- Calculate correlation strength
        max(case when severity_level = 'High' then avg_score else 0 end) -
        max(case when severity_level = 'Low' then avg_score else 0 end) as correlation_strength,

        -- Percentage of high scores in high severity patients
        round(100.0 * max(case when severity_level = 'High' then high_score_count else 0 end) /
              nullif(max(case when severity_level = 'High' then patient_count else 0 end), 0), 2) as high_severity_prevalence,

        -- Flag critical symptoms
        case
            when symptom in ('coughing_blood', 'shortness_breath', 'swallowing_difficulty') then 'Critical'
            when symptom in ('chest_pain', 'weight_loss', 'fatigue') then 'Major'
            else 'Supporting'
        end as symptom_category

    from symptom_severity_analysis
    group by symptom
),

symptom_combinations as (
    select
        severity_level,

        -- Critical symptom combinations
        sum(case when coughing_blood >= 6 and shortness_breath >= 6 then 1 else 0 end) as critical_respiratory,
        sum(case when fatigue >= 6 and weight_loss >= 6 then 1 else 0 end) as systemic_symptoms,
        sum(case when chest_pain >= 6 and shortness_breath >= 6 then 1 else 0 end) as cardio_respiratory,

        -- Symptom cluster analysis
        sum(case when severe_symptom_count >= 5 then 1 else 0 end) as multiple_severe_symptoms,

        count(patient_id) as total_patients,

        -- Average symptom burden
        round(avg(avg_symptom_severity), 2) as avg_symptom_burden

    from patient_data
    group by severity_level
)

select
    sc.*,
    rank() over (order by correlation_strength desc) as correlation_rank

from symptom_correlation sc
order by correlation_strength desc