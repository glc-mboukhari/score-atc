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