{{ config(
    materialized='table'
) }}

with patients as (
    select * from {{ ref('dim_patients') }}
),

risk_profile as (
    select * from {{ ref('int_patient_risk_profile') }}
),

symptoms as (
    select * from {{ ref('int_patient_symptoms') }}
),

fact_table as (
    select
        -- Patient dimensions
        p.patient_id,
        p.age,
        p.age_group,
        p.age_range,
        p.gender,
        p.severity_level,
        p.severity_numeric,
        p.is_senior,
        p.is_young_adult,

        -- Risk metrics
        r.avg_risk_score,
        r.overall_risk_category,
        r.high_risk_factor_count,
        r.risk_concentration,
        r.diet_quality,

        -- Individual risk levels
        r.smoking_risk_level,
        r.alcohol_risk_level,
        r.obesity_risk_level,
        r.genetic_risk_level,
        r.air_pollution_risk_level,

        -- Raw risk scores for detailed analysis
        r.smoking,
        r.alcohol_use,
        r.obesity,
        r.genetic_risk,
        r.air_pollution,
        r.occupational_hazards,
        r.passive_smoker,
        r.chronic_lung_disease,
        r.dust_allergy,
        r.balanced_diet,

        -- Symptom metrics
        s.avg_symptom_severity,
        s.symptom_severity_category,
        s.severe_symptom_count,
        s.symptom_burden,
        s.critical_symptom_status,
        s.max_critical_symptom,
        s.respiratory_symptom_avg,
        s.general_symptom_avg,

        -- Raw symptom scores for detailed analysis
        s.chest_pain,
        s.coughing_blood,
        s.fatigue,
        s.weight_loss,
        s.shortness_breath,
        s.wheezing,
        s.swallowing_difficulty,
        s.clubbing_fingernails,
        s.frequent_cold,
        s.dry_cough,
        s.snoring,

        -- Composite scores
        (r.avg_risk_score + s.avg_symptom_severity) / 2 as overall_health_score,

        -- Risk-Symptom alignment
        case
            when r.overall_risk_category = 'High' and s.symptom_severity_category = 'Severe' then 'High Risk - High Symptoms'
            when r.overall_risk_category = 'High' and s.symptom_severity_category in ('Moderate', 'Mild') then 'High Risk - Low Symptoms'
            when r.overall_risk_category in ('Medium', 'Low') and s.symptom_severity_category = 'Severe' then 'Low Risk - High Symptoms'
            else 'Low Risk - Low Symptoms'
        end as risk_symptom_profile,

        -- Timestamps
        p.loaded_at,
        current_timestamp() as created_at

    from patients p
    left join risk_profile r on p.patient_id = r.patient_id
    left join symptoms s on p.patient_id = s.patient_id
)

select * from fact_table