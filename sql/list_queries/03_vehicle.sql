drop table veh_info;
Create table veh_info AS
SELECT
    distinct veh.vehicle_id, veh.nb_options, veh.options, veh.total_options_price
FROM "dwhstats"."dwh_stats"."dim_vehicle" veh