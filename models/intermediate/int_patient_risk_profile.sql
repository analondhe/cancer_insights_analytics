{{ config(
    materialized='view'
) }}

with patients as (
    select * from {{ ref('stg_patients') }}
),

risk_calculations as (
    select
        patient_id,
        age,
        gender,
        severity_level,

        -- Individual risk factors
        smoking,
        alcohol_use,
        obesity,
        genetic_risk,
        air_pollution,
        occupational_hazards,
        passive_smoker,
        chronic_lung_disease,
        dust_allergy,

        -- Calculate average risk score
        (smoking + alcohol_use + obesity + genetic_risk + air_pollution +
         occupational_hazards + passive_smoker + chronic_lung_disease + dust_allergy) / 9.0 as avg_risk_score,

        -- Categorize individual risk factors
        {{ risk_level('smoking') }} as smoking_risk_level,
        {{ risk_level('alcohol_use') }} as alcohol_risk_level,
        {{ risk_level('obesity') }} as obesity_risk_level,
        {{ risk_level('genetic_risk') }} as genetic_risk_level,
        {{ risk_level('air_pollution') }} as air_pollution_risk_level,

        -- Count high risk factors (score >= 6)
        case when smoking >= 6 then 1 else 0 end +
        case when alcohol_use >= 6 then 1 else 0 end +
        case when obesity >= 6 then 1 else 0 end +
        case when genetic_risk >= 6 then 1 else 0 end +
        case when air_pollution >= 6 then 1 else 0 end +
        case when occupational_hazards >= 6 then 1 else 0 end +
        case when passive_smoker >= 6 then 1 else 0 end +
        case when chronic_lung_disease >= 6 then 1 else 0 end as high_risk_factor_count,

        -- Protective factors (inverse scoring - higher balanced diet is good)
        balanced_diet,
        case
            when balanced_diet >= 6 then 'Good'
            when balanced_diet >= 3 then 'Moderate'
            else 'Poor'
        end as diet_quality

    from patients
),

risk_profile as (
    select
        *,
        case
            when avg_risk_score >= 6 then 'High'
            when avg_risk_score >= 3 then 'Medium'
            else 'Low'
        end as overall_risk_category,

        case
            when high_risk_factor_count >= 5 then 'Critical'
            when high_risk_factor_count >= 3 then 'Elevated'
            when high_risk_factor_count >= 1 then 'Moderate'
            else 'Low'
        end as risk_concentration

    from risk_calculations
)

select * from risk_profile