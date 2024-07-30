drop table ann_online_nov_2023_april_2024;
Create table ann_online_nov_2023_april_2024 AS
SELECT
    ann.reference,
    ann.date_snapshot,
    ann.first_online_date,
    ann.vehicle_id,
    ann.owner_correlation_id,
    ann.initial_price,
    ann.vehicle_gearbox,
    ann.vehicle_co2,
    ann.vehicle_make,
    ann.vehicle_external_color,
    ann.vehicle_energy,
    ann.vehicle_first_hand,
    ann.price,
    ann.vehicle_year,
    ann.zip_code,
    ann.vehicle_model,
    ann.pictures_data_count_valid_photosphere,
    ann.pictures_data_count_valid,
    ann.autoviza_display,
    ann.vehicle_doors,
    ann.vehicle_commercial_name,
    ann.vehicle_internal_color,
    ann.vehicle_power_din,
    ann.vehicle_version,
    ann.vehicle_category,
    ann.vehicle_rated_horse_power,
    ann.vehicle_cubic,
    ann.manufacturer_warranty_duration,
    ann.vehicle_trunk_volume,
    ann.pictures_data_count_valid360_exterieur,
    ann.vehicle_length,
    ann.vehicle_height,
    ann.vehicle_weight,
    ann.good_deal_badge,
    ann.mileage_badge,
    ann.vehicle_brut_quotation,
    ann.good_deal_rate,
    ann.vehicle_refined_quotation,
    ann.vehicle_mileage,
    ann.vehicle_average_mileage,
    ann.vehicle_seats,
    ann.vehicle_pollution_norm,
    ann.vehicle_crit_air,
    ann.vehicle_owners,
    ann.pictures_data_count,
    ann.vehicle_reliability_index,
    ann.customer_type,
    ann.mileage_badge_value,
    ann.good_deal_badge_value,
    ann.network_warranty_duration,
    ann.constructor_warranty_end_date,
    ann.vehicle_condition,
    ann.vehicle_motorization,
    ann.vehicle_trim_level
FROM "dwhstats"."dwh_stats"."dim_annonce_lc_snapshots" ann
WHERE ann.f_status_online = TRUE  
AND CONCAT(CONCAT(CONCAT(CONCAT(ann.year, '-'), ann.month), '-'), ann.day) = CAST(DATEADD(DAY,1,ann.first_online_date) as DATE)
AND CONCAT(CONCAT(CONCAT(CONCAT(ann.year, '-'), ann.month), '-'), ann.day) BETWEEN '{{start_date}}' AND '{{end_date}}'
order by ann.date_snapshot, ann.reference asc;

SELECT year, month, day, v_label, v_version_id, v_specs_price, r_model, r_make, r_commercial_model, v_start_date, v_end_date  
FROM "dwhstats"."dwh_stats"."dim_referentiel_vehicule";

drop table veh_info;
Create table veh_info AS
SELECT
    distinct veh.vehicle_id, veh.nb_options, veh.options, veh.total_options_price
FROM "dwhstats"."dwh_stats"."dim_vehicle" veh;

drop table listing_nov_2023_april_2024;
Create table listing_nov_2023_april_2024 AS
select 
    search_temp.classified_ref,
    search_temp.date_snapshot as event_date,
    search_temp.nb_occurence_total as toto
from "dwhstats"."externaldb_data_datalakehouse"."dim_event_annonce_total_count" search_temp 
where CONCAT(CONCAT(CONCAT(CONCAT(search_temp.year, '-'), search_temp.month), '-'), search_temp.day) BETWEEN '{{start_date}}' AND '{{end_date}}'
and search_temp.event_type in ('CLASSIFIED_DISPLAYED') AND search_temp.event_page = 'LISTING'
and coalesce(search_temp.completion_quality,'0') in ('-1','0')
and search_temp.event_source not in ('classified:lcpab:vitrines:lc','classified:promoneuve:web','classified:promoneuve:mobile');

drop table detail_nov_2023_april_2024;
Create table detail_nov_2023_april_2024 AS
select 
    search_temp.classified_ref,
    search_temp.date_snapshot as event_date,
    search_temp.nb_occurence_total as toto
from "dwhstats"."externaldb_data_datalakehouse"."dim_event_annonce_total_count" search_temp 
where CONCAT(CONCAT(CONCAT(CONCAT(search_temp.year, '-'), search_temp.month), '-'), search_temp.day) BETWEEN '{{start_date}}' AND '{{end_date}}'
and search_temp.event_type in ('CLASSIFIED_DISPLAYED') AND search_temp.event_page = 'DETAIL'
and coalesce(search_temp.completion_quality,'0') in ('-1','0')
and search_temp.event_source not in ('classified:lcpab:vitrines:lc','classified:promoneuve:web','classified:promoneuve:mobile');

drop table ic_nov_2023_april_2024;
Create table ic_nov_2023_april_2024 AS
select 
    search_temp.classified_ref,
    search_temp.date_snapshot as event_date,
    search_temp.nb_occurence_total as toto
from "dwhstats"."externaldb_data_datalakehouse"."dim_event_annonce_total_count" search_temp 
where CONCAT(CONCAT(CONCAT(CONCAT(search_temp.year, '-'), search_temp.month), '-'), search_temp.day) BETWEEN '{{start_date}}' AND '{{end_date}}'
and search_temp.event_type in ('PHONE_CLICKED','EDIT_MAIL_CLICKED')
and coalesce(search_temp.completion_quality,'0') in ('-1','0')
and search_temp.event_source not in ('classified:lcpab:vitrines:lc','classified:promoneuve:web','classified:promoneuve:mobile');

drop table package_nov_2023_april_2024;
Create table package_nov_2023_april_2024 AS
SELECT
    bnn.ccl_numero,
    bnn.date_snapshot,
    bnn.ccl_type_de_bien,
    bnn.Niveau_Pack
FROM (
    select 
        ccl_numero,
        date_snapshot, 
        ccl_type_de_bien,
        rank() over(partition by ccl_numero order by date_snapshot asc) as rank_event,
        case when Niveau_Pack = 'FULL STOCK' then 'MOTO' 
            when Niveau_Pack in ('Forfait 7 annonces Internet', 'Forfait 20 annonces Internet', 'Forfait 50 annonces Internet', 'Forfait supérieur à 50 annonces Internet') then 'FAI'
            when Niveau_Pack in ('Forfait 2 annonces', 'Pack 8 annonces', 'Pack 5 annonces', 'Pack 2 annonces') then 'FA'
            when Niveau_Pack = 'Tarif Petits Prix' then 'PETITS PRIX'
            else Niveau_Pack end as Niveau_Pack
    from (
        select ccl_numero, date_ref.date_snapshot, ccl_type_de_bien, produit_type_vitrine, produit_nom, 
            max(case when ccl_type_de_bien = 'MOTO' then 'Pack MOTO' else coalesce(produit_type_vitrine, produit_nom) end ) as Niveau_Pack
                from dwh_stats.fct_commande_tr 
                left join dwh_stats.dim_produit using(pk_produit_id)
                -- On recupère les dates de snapshot 
                inner join (select distinct date_snapshot from "dwhstats"."dwh_stats"."dim_annonce_lc_snapshots" ann 
                            where ann."f_status_online" = true and CONCAT(CONCAT(CONCAT(CONCAT(ann.year, '-'), ann.month), '-'), ann.day) BETWEEN '{{start_date}}' AND '{{end_date}}'
                            ) date_ref on commande_date_de_debut <= date_ref.date_snapshot and commande_date_de_fin >= date_ref.date_snapshot 
                where commande_site_label='La Centrale' and (commande_motif_de_fermeture is null or commande_motif_de_fermeture not in ('Annulation de commande','Annulation de la commande','Erreur'))
                                                        -- and (product_famille_de_produit ='Pack Annonces' or ccl_type_de_bien='MOTO') 
                                                        and product_famille_de_produit in ('Pack Annonces','PAI','Tarif', 'Pack Petits Prix','Vitrine')
                                                        --and produit_type_vitrine is not NULL
                    group by ccl_numero, date_ref.date_snapshot, ccl_type_de_bien, produit_type_vitrine, produit_nom)
    group by ccl_numero, date_snapshot, ccl_type_de_bien,
            case when Niveau_Pack = 'FULL STOCK' then 'MOTO' 
                when Niveau_Pack  in ('Forfait 7 annonces Internet', 'Forfait 20 annonces Internet', 'Forfait 50 annonces Internet', 'Forfait supérieur à 50 annonces Internet') then 'FAI'
                when Niveau_Pack  in ('Forfait 2 annonces', 'Pack 8 annonces', 'Pack 5 annonces', 'Pack 2 annonces') then 'FA'
                when Niveau_Pack = 'Tarif Petits Prix' then 'PETITS PRIX'
                else Niveau_Pack end
    ) bnn
where bnn.rank_event = 1;

drop table package_price_nov_2023_april_2024;
Create table package_price_nov_2023_april_2024 AS
SELECT
    bnn.ccl_numero,
    bnn.date_snapshot,
    bnn.nb_place_parking,
    bnn.selection_pack,
    bnn.total_price,
    bnn.total_price_hors_option
FROM (
    select ccl_numero, date_ref.date_snapshot,
        case when max(commande_volume_forfait) > 0 then max(commande_volume_forfait) else sum(case when product_famille_de_produit = 'Tarif Petits Prix' then commande_quantity else 0 end) end as nb_place_parking, 
        max(case when product_famille_de_produit in ('Pack Annonces','PAI','Tarif', 'Pack Petits Prix','Vitrine') then 1 else 0 end) as selection_pack,
        sum(commande_prix_total) as total_price,
        sum(case when product_famille_de_produit in ('Pack Annonces','PAI','Tarif', 'Pack Petits Prix','Tarif Petits Prix','Vitrine') then commande_prix_total else null end) as total_price_hors_option,
        rank() over(partition by ccl_numero order by date_ref.date_snapshot) as rank_event
    from dwh_stats.fct_commande_tr 
    left join dwh_stats.dim_produit using(pk_produit_id)
    inner join (select distinct date_snapshot from "dwhstats"."dwh_stats"."dim_annonce_lc_snapshots" ann 
                    where ann."f_status_online" = true and CONCAT(CONCAT(CONCAT(CONCAT(ann.year, '-'), ann.month), '-'), ann.day) BETWEEN '{{start_date}}' AND '{{end_date}}'
        ) date_ref on commande_date_de_debut <= date_ref.date_snapshot and commande_date_de_fin >= date_ref.date_snapshot 
    where commande_site_label='La Centrale' and (commande_motif_de_fermeture is null or commande_motif_de_fermeture not in ('Annulation de commande','Annulation de la commande','Erreur'))
        and product_famille_de_produit in ('Pack Annonces','PAI','Tarif', 'Pack Petits Prix','Tarif Petits Prix','Vitrine','Option de visibilité BoostVO','Call Tracking','Livraison','ProPulse')
    group by ccl_numero, date_ref.date_snapshot
    ) bnn
where bnn.rank_event = 1;

drop table intermediaire_ann_ic_nov_2023_april_2024;
Create table intermediaire_ann_ic_nov_2023_april_2024 AS
SELECT 
    bnn.reference,
    sum(bnn.toto) as total_ic
FROM (
    SELECT  
        ann.*,
        ic.event_date,
        ic.toto,
        rank() over(partition by ann.reference order by ic.event_date desc) as rank_event
    FROM ann_online_nov_2023_april_2024 ann
    LEFT JOIN ic_nov_2023_april_2024 ic ON ann.reference = ic.classified_ref
    WHERE ic.event_date between ann.date_snapshot and dateadd(day,45,ann.date_snapshot)
    ) bnn
where bnn.rank_event = 1
group by bnn.reference;

drop table intermediaire_ann_listing_nov_2023_april_2024;
Create table intermediaire_ann_listing_nov_2023_april_2024 AS
SELECT 
    bnn.reference,
    sum(bnn.toto) as total_listing
FROM (
    SELECT  
        ann.*,
        listing.event_date,
        listing.toto,
        rank() over(partition by ann.reference order by listing.event_date desc) as rank_event
    FROM ann_online_nov_2023_april_2024 ann
    LEFT JOIN listing_nov_2023_april_2024 listing ON ann.reference = listing.classified_ref
    WHERE listing.event_date between ann.date_snapshot and dateadd(day,45,ann.date_snapshot)
    ) bnn
where bnn.rank_event = 1
group by bnn.reference

drop table intermediaire_ann_detail_nov_2023_april_2024;
Create table intermediaire_ann_detail_nov_2023_april_2024 AS
SELECT 
    bnn.reference,
    sum(bnn.toto) as total_detail
FROM(
    SELECT  
        ann.*,
        detail.event_date,
        detail.toto,
        rank() over(partition by ann.reference order by detail.event_date desc) as rank_event
    FROM ann_online_nov_2023_april_2024 ann
    LEFT JOIN detail_nov_2023_april_2024 detail ON ann.reference = detail.classified_ref
    WHERE detail.event_date between ann.date_snapshot and dateadd(day,45,ann.date_snapshot)
    ) bnn
where bnn.rank_event = 1
group by bnn.reference;

drop table final_ann_nov_2023_april_2024;
Create table final_ann_nov_2023_april_2024 AS
SELECT
    ann.*,
    veh.nb_options,
    veh.options,
    veh.total_options_price,
    ann_ic.total_ic as nb_id,
    ann_listing.total_listing as nb_listing,
    ann_detail.total_detail as nb_detail,
    pack.ccl_type_de_bien,
    pack.Niveau_Pack,
    pack_price.nb_place_parking,
    pack_price.selection_pack,
    pack_price.total_price,
    pack_price.total_price_hors_option
FROM ann_online_nov_2023_april_2024 ann
LEFT JOIN veh_info veh on ann.vehicle_id = veh.vehicle_id
LEFT JOIN intermediaire_ann_ic_nov_2023_april_2024 ann_ic ON ann.reference = ann_ic.reference
LEFT JOIN intermediaire_ann_listing_nov_2023_april_2024 ann_listing ON ann.reference = ann_listing.reference
LEFT JOIN intermediaire_ann_detail_nov_2023_april_2024 ann_detail ON ann.reference = ann_detail.reference
LEFT JOIN package_nov_2023_april_2024 pack ON UPPER(ann.owner_correlation_id) = pack.ccl_numero 
LEFT JOIN package_price_nov_2023_april_2024 pack_price ON UPPER(ann.owner_correlation_id) = pack_price.ccl_numero;