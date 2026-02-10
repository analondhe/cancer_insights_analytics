{{ config(
    materialized='view'
) }}

with patients as (
    select * from {{ ref('stg_patients') }}
),

symptom_aggregation as (
    select
        patient_id,
        age,
        gender,
        severity_level,

        -- Individual symptoms
        chest_pain,
        coughing_blood,
        fatigue,
        weight_loss,
        shortness_breath,
        wheezing,
        swallowing_difficulty,
        clubbing_fingernails,
        frequent_cold,
        dry_cough,
        snoring,

        -- Calculate average symptom severity
        (chest_pain + coughing_blood + fatigue + weight_loss + shortness_breath +
         wheezing + swallowing_difficulty + clubbing_fingernails + frequent_cold +
         dry_cough + snoring) / 11.0 as avg_symptom_severity,

        -- Count severe symptoms (score >= 6)
        case when chest_pain >= 6 then 1 else 0 end +
        case when coughing_blood >= 6 then 1 else 0 end +
        case when fatigue >= 6 then 1 else 0 end +
        case when weight_loss >= 6 then 1 else 0 end +
        case when shortness_breath >= 6 then 1 else 0 end +
        case when wheezing >= 6 then 1 else 0 end +
        case when swallowing_difficulty >= 6 then 1 else 0 end +
        case when clubbing_fingernails >= 6 then 1 else 0 end +
        case when frequent_cold >= 6 then 1 else 0 end +
        case when dry_cough >= 6 then 1 else 0 end +
        case when snoring >= 6 then 1 else 0 end as severe_symptom_count,

        -- Critical symptoms (blood, breathing, swallowing)
        greatest(coughing_blood, shortness_breath, swallowing_difficulty) as max_critical_symptom,

        -- Respiratory symptoms aggregate
        (shortness_breath + wheezing + dry_cough + coughing_blood) / 4.0 as respiratory_symptom_avg,

        -- General symptoms aggregate
        (fatigue + weight_loss + chest_pain + frequent_cold) / 4.0 as general_symptom_avg

    from patients
),

symptom_profile as (
    select
        *,
        case
            when avg_symptom_severity >= 6 then 'Severe'
            when avg_symptom_severity >= 3 then 'Moderate'
            else 'Mild'
        end as symptom_severity_category,

        case
            when severe_symptom_count >= 7 then 'Very High'
            when severe_symptom_count >= 4 then 'High'
            when severe_symptom_count >= 2 then 'Moderate'
            else 'Low'
        end as symptom_burden,

        case
            when max_critical_symptom >= 7 then 'Critical'
            when max_critical_symptom >= 5 then 'Concerning'
            else 'Stable'
        end as critical_symptom_status

    from symptom_aggregation
)

select * from symptom_profile