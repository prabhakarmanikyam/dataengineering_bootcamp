-- checking if game_details have any duplicates
select game_id, 
		team_id, 
		player_id, 
		count(1) 
from game_details 
group by 1,2,3 
having count(1) > 1

-- deduplicate game_details table

Delete from game_details 
where ctid in(
		select ctid from (
				select ctid,
				row_number() over (partition by game_id, team_id, player_id order by ctid) as row_num
			from game_details
)sub
 where row_num >1
);


-- to retriev data from devices
select * from devices


-- DDL for an user_devices_cumulated

create table user_devices_cumulated(
		user_id text,
		browser_type text,
		device_activity_date_list DATE[],
		date DATE,
		primary key(user_id, browser_type, date)
)

-- cumulative query to generate data in device_activity_date_list

insert into user_devices_cumulated
with yesterday as(
		select * from user_devices_cumulated
		where date = DATE('2023-01-30')
),
today as(
		SELECT cast(e.user_id as text),
		DATE(CAST(e.event_time as TIMESTAMP)) as date_active,
		d.browser_type as browser_type
		from
		events e JOIN devices d ON e.device_id = d.device_id where DATE(CAST(e.event_time as TIMESTAMP)) = DATE('2023-01-31') and e.user_id is not null
		group by e.user_id, DATE(CAST(e.event_time as TIMESTAMP)), d.browser_type
)
select 
		coalesce(t.user_id,y.user_id) as user_id,
		coalesce(t.browser_type, y.browser_type) as browser_type,
		case when y.device_activity_date_list is null 
		then array[t.date_active] 
		when t.date_active is null then y.device_activity_date_list
		else array[t.date_active] || y.device_activity_date_list
		end as device_activity_date_list,
		coalesce(t.date_active,y.date + Interval '1 day') as date
from today t full outer join yesterday y on t.user_id = y.user_id and t.browser_type = y.browser_type

-- to retrieve data from user_devices_cumulated
select * from user_devices_cumulated

-- converting the device-activity_list into datelist_int
WITH users as (
 		select * 
		from user_devices_cumulated 
		where date = DATE('2023-01-31')
), series as(
		SELECT * 
		from generate_series(DATE('2023-01-01'),DATE('2023-01-31'), INTERVAL '1 day') 
		as series_date
),
place_holder_ints as(
select 
 case 
  when device_activity_date_list @> array[DATE(series_date)]
   then cast(pow(2, 32 - (date - DATE(series_date))) as bigint) 
   else 0
   end as place_holder_int_value,
 *
from users cross join series
)
select user_id,
		browser_type,
		cast(cast(SUM(place_holder_int_value) as bigint) as bit(32)) as datelist_int
from place_holder_ints
group by user_id, browser_type


-- Creating DDl for hosts-cumulated table

create table hosts_cumulated(
			host text,
			host_activity_datelist DATE[],
			c_date DATE,
			primary key(host, c_date)
)

-- incremental query to generate host-activity_datelist

insert into hosts_cumulated
With yesterday as(
	select * 
	from hosts_cumulated 
	where c_date = DATE('2023-01-02')
),

today as(
	select host,
		DATE(cast(event_time as TIMESTAMP)) as date_active
		from events
		where DATE(cast(event_time as TIMESTAMP)) = DATE('2023-01-03')
		group by host, DATE(cast(event_time as TIMESTAMP))
)
select 
		coalesce(t.host, y.host) as host,
		case when y.host_activity_datelist is null then ARRAY[t.date_active]
			when t.date_active is null then y.host_activity_datelist
			else ARRAY[t.date_active] || y.host_activity_datelist
			END as host_activity_datelist,
			coalesce(t.date_active, y.c_date + Interval '1 day') as c_date
		from today t full outer join yesterday y on t.host = y.host

-- to retrieve data from hosts_cumulated
select * from hosts_cumulated



-- creating monthly reduced fact table DDL for host_activity_reduced

create table host_activity_reduced(
	month_start DATE,
	host text,
	hit_array REAL[],
	unique_visitors_array REAL[],
	PRIMARY KEY(host, month_start)
)


-- Incremental query to load table host_activity_reduced
INSERT INTO host_activity_reduced
with daily_aggregate as (
		select host,
		DATE(cast(event_time as TIMESTAMP)) as date,
		count(*) as hits,
		count(distinct user_id) as unique_visitors
		from events 
		where DATE(cast(event_time as timestamp)) = DATE('2023-01-02')
		group by host, date
),
yesterday_array as(
		select * from 
		host_activity_reduced where month_start = DATE('2023-01-01')
)
select  coalesce(ya.month_start, DATE_TRUNC('month', da.date)) as month_start,
		coalesce(ya.host, da.host) as host,
		case when ya.hit_array is not null then 
		       ya.hit_array || ARRAY[coalesce(da.hits,0)]
			  when ya.hit_array is null then  ARRAY_FILL(0, ARRAY[coalesce(date - DATE(DATE_TRUNC('month',date)),0)]) || array[coalesce(da.hits,0)]
			  end as hit_array,
		case when ya.unique_visitors_array is not null then 
		       ya.unique_visitors_array || ARRAY[coalesce(da.unique_visitors,0)]
			  when ya.unique_visitors_array is null then  ARRAY_FILL(0, ARRAY[coalesce(date - DATE(DATE_TRUNC('month',date)),0)]) || array[coalesce(da.unique_visitors,0)]
			  end as unique_visitors_array
		from daily_aggregate da
 			FULL OUTER JOIN yesterday_array ya ON
			 da.host = ya.host
			ON CONFLICT(host, month_start)
			DO 
			UPDATE SET hit_array = EXCLUDED.hit_array




-- to retrieve data from host_activity_reduced
select * from host_activity_reduced




