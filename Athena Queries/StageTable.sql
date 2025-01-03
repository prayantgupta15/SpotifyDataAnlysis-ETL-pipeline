-- SELECT * FROM "AwsDataCatalog"."sptifydb"."stagingsource_raw_data_parquetfiles" 
-- order by data_last_refreshed desc 
-- limit 10;
-----------------------------------------------------------------------------------------------

-- => Add partitions:

MSCK REPAIR TABLE stagingsource_raw_data_parquetfiles;

-----------------------------------------------------------------------------------------------
-- => Delete partition:

-- alter table stagingsource_raw_data_parquetfiles
-- drop partition (year=2025,month=1,day=3);

-----------------------------------------------------------------------------------------------
-- => test:

-- select data_last_refreshed, artist_name from stagingsource_raw_data_parquetfiles;
-- group by artist_name;
-----------------------------------------------------------------------------------------------

MSCK REPAIR TABLE AwsDataCatalog.sptifydb.stagingsource_raw_data_parquetfiles;
