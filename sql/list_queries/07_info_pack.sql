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
                            where ann."f_status_online" = true and CONCAT(CONCAT(CONCAT(CONCAT(ann.year, '-'), ann.month), '-'), ann.day) BETWEEN '2023-11-01' AND '2024-05-16'
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
where bnn.rank_event = 1