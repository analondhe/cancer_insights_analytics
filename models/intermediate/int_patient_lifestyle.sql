-- Lifestyle factors that patients can influence
-- Based on analysis: Higher scores (1-8) correlate with higher severity

with patients as (
    select * from {{ ref('stg_patients') }}
),

lifestyle as (
    select
        patient_id,
        age,
        {{ age_bucket('age') }} as age_group,
        gender,

        -- Lifestyle factors (raw scores 1-8)
        balanced_diet,      -- Avg: Low=3.0, High=6.6 (counterintuitive)
        alcohol_use,        -- Avg: Low=2.2, High=6.8 (strong correlation)
        obesity,            -- Avg: Low=2.4, High=6.7 (strong correlation)
        smoking,            -- Avg: Low=3.0, High=6.1 (strong correlation)
        snoring,            -- Avg: Low=2.1, High=3.2 (weak correlation)

        severity_level

    from patients
)

select * from lifestyle
