-- => creating stage view from stage table to add surrogate column to get primary key for MERGE ON condition
-- create or replace view stageView as (

-- select 
-- row_number() over(partition by data_last_refreshed order by track_id,artist_id ) unique_no, --surrogate key
-- cast(data_last_refreshed as varchar(30)) data_last_refreshed,
-- -- cast(data_last_refreshed as str) data_last_refreshed,
-- followers,
-- added_at,
-- artist_id,
-- artist_name,
-- disc_number,
-- duration_ms,
-- track_id,
-- track_name,
-- popularity,
-- track_number,
-- album_type,
-- album_name,
-- release_date,
-- total_tracks,
-- year,month ,day
--  from stagingsource_raw_data_parquetfiles
-- );

-----------------------------------------------------------------------------------------------
-- => create iceberg table using Stage view

-- create table spotifyIcebergMORathena WITH(
-- table_type='iceberg',
-- format = 'PARQUET',
-- partitioning = ARRAY['day(data_last_refreshed_ts6)'],
-- location =  's3://prayantdatasetbucket/myspotifyAPIData/spotifyIcebergMORAthena/',
-- is_external = false
-- )
-- as select cast(data_last_refreshed as TIMESTAMP(6)) data_last_refreshed_ts6,* from stageView;


-----------------------------------------------------------------------------------------------
-- => Load Stage data to iceberg table
merge into spotifyIcebergMORathena  a
using 
(select cast(data_last_refreshed as TIMESTAMP(6)) data_last_refreshed_ts6,* from stageView) AS cte
on a.data_last_refreshed_ts6 = cte.data_last_refreshed_ts6 and a.unique_no = cte.unique_no
when MATCHED
        THEN UPDATE SET 
        -- data_last_refreshed='hogya'
        data_last_refreshed_ts6 = cte.data_last_refreshed_ts6
when not matched then insert values (
cte.data_last_refreshed_ts6,
cte.unique_no,
cte.data_last_refreshed,
cte.followers,
cte.added_at,
cte.artist_id,
cte.artist_name,
cte.disc_number,
cte.duration_ms,
cte.track_id,
cte.track_name,
cte.popularity,
cte.track_number,
cte.album_type,
cte.album_name,
cte.release_date,
cte.total_tracks,
cte.year,cte.month ,cte.day
);

-----------------------------------------------------------------------------------------------


-- testing output
-- select * from spotifyIcebergMORathena order by data_last_refreshed, unique_no;