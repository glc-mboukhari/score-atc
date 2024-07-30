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
LEFT JOIN package_price_nov_2023_april_2024 pack_price ON UPPER(ann.owner_correlation_id) = pack_price.ccl_numero