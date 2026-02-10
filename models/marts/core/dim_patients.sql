{{ config(
    materialized='table'
) }}

with patients as (
    select * from {{ ref('stg_patients') }}
),

patient_dimension as (
    select
        patient_id,
        age,
        {{ age_bucket('age') }} as age_group,
        gender,
        severity_level,

        -- Create age range for more detailed analysis
        case
            when age < 30 then 'Under 30'
            when age between 30 and 39 then '30-39'
            when age between 40 and 49 then '40-49'
            when age between 50 and 59 then '50-59'
            when age between 60 and 69 then '60-69'
            when age >= 70 then '70+'
            else 'Unknown'
        end as age_range,

        -- Create severity numeric for easier calculations
        case
            when severity_level = 'High' then 3
            when severity_level = 'Medium' then 2
            when severity_level = 'Low' then 1
            else 0
        end as severity_numeric,

        -- Demographics flags
        case when age >= 65 then true else false end as is_senior,
        case when age < 40 then true else false end as is_young_adult,

        loaded_at,
        current_timestamp() as updated_at

    from patients
)

select * from patient_dimension