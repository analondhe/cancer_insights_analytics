{{ config(
    materialized='view'
) }}

select
    "Patient Id" as patient_id,
    "Age" as age,
    "Gender" as gender,
    "Air Pollution" as air_pollution,
    "Alcohol use" as alcohol_use,
    "Dust Allergy" as dust_allergy,
    "OccuPational Hazards" as occupational_hazards,
    "Genetic Risk" as genetic_risk,
    "chronic Lung Disease" as chronic_lung_disease,
    "Balanced Diet" as balanced_diet,
    "Obesity" as obesity,
    "Smoking" as smoking,
    "Passive Smoker" as passive_smoker,
    "Chest Pain" as chest_pain,
    "Coughing of Blood" as coughing_blood,
    "Fatigue" as fatigue,
    "Weight Loss" as weight_loss,
    "Shortness of Breath" as shortness_breath,
    "Wheezing" as wheezing,
    "Swallowing Difficulty" as swallowing_difficulty,
    "Clubbing of Finger Nails" as clubbing_fingernails,
    "Frequent Cold" as frequent_cold,
    "Dry Cough" as dry_cough,
    "Snoring" as snoring,
    "Level" as severity_level,
    current_timestamp as loaded_at
from {{ ref('cancer_patient_data_sets') }}