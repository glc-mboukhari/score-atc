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
group by bnn.reference