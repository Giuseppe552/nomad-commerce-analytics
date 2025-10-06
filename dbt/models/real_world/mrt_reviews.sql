{{ config(materialized='table', tags=['real','mart']) }}

-- Review distributions and delay penalty
-- Columns:
--   as_of_date, reviews_count, avg_score, late_avg_score, ontime_avg_score, delay_penalty

with rev as (
  select
    date_trunc('day', review_creation_ts)::date as as_of_date,
    order_id,
    score
  from {{ ref('stg_reviews') }}
  where score is not null
),

del as (
  select order_id, is_late, is_on_time
  from {{ ref('fct_deliveries') }}
),

joined as (
  select
    r.as_of_date,
    r.order_id,
    r.score,
    d.is_late,
    d.is_on_time
  from rev r
  left join del d using (order_id)
),

by_day as (
  select
    as_of_date,
    count(*)                                                    as reviews_count,
    avg(score)::double                                          as avg_score,
    avg(case when is_late then score end)::double               as late_avg_score,
    avg(case when is_on_time then score end)::double            as ontime_avg_score
  from joined
  group by 1
)

select
  as_of_date,
  reviews_count,
  avg_score,
  late_avg_score,
  ontime_avg_score,
  case
    when late_avg_score is not null and ontime_avg_score is not null
    then ontime_avg_score - late_avg_score
    else null
  end as delay_penalty
from by_day
order by as_of_date;
