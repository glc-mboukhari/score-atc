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
                    where ann."f_status_online" = true and CONCAT(CONCAT(CONCAT(CONCAT(ann.year, '-'), ann.month), '-'), ann.day) BETWEEN '2023-11-01' AND '2024-05-16'
        ) date_ref on commande_date_de_debut <= date_ref.date_snapshot and commande_date_de_fin >= date_ref.date_snapshot 
    where commande_site_label='La Centrale' and (commande_motif_de_fermeture is null or commande_motif_de_fermeture not in ('Annulation de commande','Annulation de la commande','Erreur'))
        and product_famille_de_produit in ('Pack Annonces','PAI','Tarif', 'Pack Petits Prix','Tarif Petits Prix','Vitrine','Option de visibilité BoostVO','Call Tracking','Livraison','ProPulse')
    group by ccl_numero, date_ref.date_snapshot
    ) bnn
where bnn.rank_event = 1