{{ config(materialized='table', tags=['real','mart']) }}

-- Daily KPIs (delivered orders only)
-- Columns:
--   kpi_date, orders_delivered, gmv, aov, freight_pct_gmv, on_time_pct, late_pct

with delivered as (
  select
    order_id,
    order_date,
    gmv,
    freight_total
  from {{ ref('fct_orders') }}
  where is_delivered
),

on_time as (
  select
    order_id,
    is_on_time,
    is_late
  from {{ ref('fct_deliveries') }}
),

by_day as (
  select
    d.order_date                                     as kpi_date,
    count(*)                                         as orders_delivered,
    sum(d.gmv)::double                               as gmv,
    sum(d.freight_total)::double                     as freight_total,
    avg(case when o.is_on_time then 1 else 0 end)    as on_time_ratio,
    avg(case when o.is_late then 1 else 0 end)       as late_ratio
  from delivered d
  left join on_time o using (order_id)
  group by 1
)

select
  kpi_date,
  orders_delivered,
  gmv,
  case when orders_delivered > 0 then gmv / orders_delivered else null end as aov,
  case when gmv > 0 then freight_total / gmv else null end                 as freight_pct_gmv,
  on_time_ratio                                                            as on_time_pct,
  late_ratio                                                               as late_pct
from by_day
order by kpi_date;
