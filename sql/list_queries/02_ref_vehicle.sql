SELECT 
    year, 
    month, 
    day, 
    v_label, 
    v_version_id, 
    v_specs_price, 
    r_model, 
    r_make, 
    r_commercial_model, 
    v_start_date, 
    v_end_date  
FROM "dwhstats"."dwh_stats"."dim_referentiel_vehicule"
