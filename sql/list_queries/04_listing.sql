drop table listing_nov_2023_april_2024;
Create table listing_nov_2023_april_2024 AS
select 
    search_temp.classified_ref,
    search_temp.date_snapshot as event_date,
    search_temp.nb_occurence_total as toto
from "dwhstats"."externaldb_data_datalakehouse"."dim_event_annonce_total_count" search_temp 
where CONCAT(CONCAT(CONCAT(CONCAT(search_temp.year, '-'), search_temp.month), '-'), search_temp.day) BETWEEN '2023-11-01' AND '2024-05-16'
and search_temp.event_type in ('CLASSIFIED_DISPLAYED') AND search_temp.event_page = 'LISTING'
and coalesce(search_temp.completion_quality,'0') in ('-1','0')
and search_temp.event_source not in ('classified:lcpab:vitrines:lc','classified:promoneuve:web','classified:promoneuve:mobile')
