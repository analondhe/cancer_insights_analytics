-- External/environmental factors outside patient's direct control
-- Based on analysis: Higher scores (1-8) correlate with higher severity

with patients as (
    select * from {{ ref('stg_patients') }}
),

non_lifestyle as (
    select
        patient_id,
        age,
        {{ age_bucket('age') }} as age_group,
        gender,

        -- Environmental factors
        air_pollution,          -- Avg: Low=2.6, High=5.7
        dust_allergy,           -- Avg: Low=3.1, High=6.6
        occupational_hazards,   -- Avg: Low=3.0, High=6.5

        -- Genetic/Medical factors
        genetic_risk,           -- Avg: Low=2.7, High=6.4
        chronic_lung_disease,   -- Avg: Low=3.1, High=5.8
        passive_smoker,         -- Avg: Low=2.6, High=6.5

        severity_level

    from patients
)

select * from non_lifestyle
