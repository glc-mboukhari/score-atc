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
group by bnn.reference