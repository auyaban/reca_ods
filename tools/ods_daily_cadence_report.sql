create or replace view public.ods_daily_cadence_report as
with ordered as (
  select
    id,
    created_at,
    (created_at at time zone 'America/Bogota')::date as usage_day,
    lag(created_at) over (
      partition by (created_at at time zone 'America/Bogota')::date
      order by created_at
    ) as prev_created_at
  from public.ods
  where (created_at at time zone 'America/Bogota')::date <= (now() at time zone 'America/Bogota')::date
),
daily_entries as (
  select
    usage_day,
    count(*) as entries_count,
    min(created_at at time zone 'America/Bogota') as first_created_at_bogota,
    max(created_at at time zone 'America/Bogota') as last_created_at_bogota
  from ordered
  group by usage_day
),
daily_gaps as (
  select
    usage_day,
    count(*) as gaps_count,
    round(avg(extract(epoch from (created_at - prev_created_at)))::numeric, 2) as avg_gap_seconds,
    round((avg(extract(epoch from (created_at - prev_created_at))) / 60.0)::numeric, 2) as avg_gap_minutes,
    round(
      percentile_cont(0.5) within group (order by extract(epoch from (created_at - prev_created_at)))::numeric,
      2
    ) as median_gap_seconds,
    round(
      (
        percentile_cont(0.5) within group (order by extract(epoch from (created_at - prev_created_at))) / 60.0
      )::numeric,
      2
    ) as median_gap_minutes,
    round(min(extract(epoch from (created_at - prev_created_at)))::numeric, 2) as min_gap_seconds,
    round(max(extract(epoch from (created_at - prev_created_at)))::numeric, 2) as max_gap_seconds
  from ordered
  where prev_created_at is not null
  group by usage_day
)
select
  e.usage_day,
  e.entries_count,
  coalesce(g.gaps_count, 0) as gaps_count,
  g.avg_gap_seconds,
  g.avg_gap_minutes,
  g.median_gap_seconds,
  g.median_gap_minutes,
  g.min_gap_seconds,
  g.max_gap_seconds,
  e.first_created_at_bogota,
  e.last_created_at_bogota
from daily_entries e
left join daily_gaps g
  on g.usage_day = e.usage_day
order by e.usage_day desc;

select *
from public.ods_daily_cadence_report
order by usage_day desc;
