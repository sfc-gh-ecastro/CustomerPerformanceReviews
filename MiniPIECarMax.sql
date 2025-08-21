---LRQ Results ------

WITH 
  -- Filter job_etl_v table first to reduce the data volume
  filtered_jobs AS (
    SELECT 
      DEPLOYMENT,
      role_name,
      user_name,
      database_name,
      schema_name,
      warehouse_name,
      warehouse_id,
      account_id,
      QUERY_PARAMETERIZED_HASH,
      SQL_TEXT_HASH,
      uuid,
      CREATED_ON,
      dur_compiling,
      DUR_XP_EXECUTING,
      error_code,
      error_message,
      CLOUD_SERVICES_CREDITS_USED,
      stats,
      description,
      TOTAL_DURATION
    FROM 
      snowhouse_import.prod.job_etl_v
    WHERE 
      CREATED_ON BETWEEN '2025-06-20 00:00:00' AND '2025-07-06 23:59:59'
      and (TOTAL_DURATION/ 60000) > 30
  ),
  -- Join filtered_jobs with account_etl_v
  jobs_with_account AS (
    SELECT 
      j.DEPLOYMENT,
      a.name,
      a.alias,
      j.role_name,
      j.user_name,
      j.database_name,
      j.schema_name,
      j.warehouse_name,
      j.warehouse_id,
      j.account_id,
      j.QUERY_PARAMETERIZED_HASH AS sql_hash,
      j.SQL_TEXT_HASH,
      j.uuid AS query_id,
      j.CREATED_ON AS RUN_DATE,
      j.dur_compiling / 1000::number AS COMPILING_SECONDS,
      j.DUR_XP_EXECUTING / 60000 AS EXECUTING_MINUTES,
      j.error_code,
      j.error_message,
      j.CLOUD_SERVICES_CREDITS_USED,
      j.stats,
      j.description,
      J.DUR_XP_EXECUTING,
      j.TOTAL_DURATION
    FROM 
      filtered_jobs j
    JOIN 
      SNOWHOUSE_IMPORT.PROD.ACCOUNT_ETL_V a 
    ON 
      a.deployment = j.deployment AND a.id = j.account_id
    WHERE 
      --a.replication_group = '1003_2ffa4dfd-9b95-4a99-8e82-81abe31d00fe'
      a.replication_group = '2001_6a16d638-cb22-49e0-9d5a-b3e9f47e041f' --CarMax
      --a.replication_group = '1015_43de5926-57f9-41c9-8d67-7d2a263e3b21' -- Cargill
      --a.replication_group = '3004_2dfc25fc-4b8d-472c-8541-77fae048b52f' --Solenis
      ----a.replication_group = '1003_2ffa4dfd-9b95-4a99-8e82-81abe31d00fe' --Fidelity
      --a.replication_group = '1003_bedbe564-2f68-4d75-8596-e430ab42cb83' --PIMCO
      --and j.ERROR_CODE is not null
  )

SELECT 
  j.DEPLOYMENT,
  j.name,
  j.alias,
  j.role_name,
  j.user_name,
  j.database_name,
  j.schema_name,
  j.warehouse_name,
  wh.warehouse_type,
  wh.size,
  j.stats:stats:warehouseSize::number AS wh_sz_Number,
  j.stats:stats:totalMemory::number AS total_memory,
  j.stats:stats:serverCount::number AS server_count,
  j.sql_hash,
  TO_VARCHAR(j.SQL_TEXT_HASH),
  j.query_id,
  j.RUN_DATE,
  j.COMPILING_SECONDS,
  j.EXECUTING_MINUTES,
  j.error_code,
  j.error_message,
  j.CLOUD_SERVICES_CREDITS_USED,
  jc.CREDITS,
  jc.XP_CREDITS,
  jc.GS_CREDITS,
  jc.XP_MICRO_CREDITS,
  jc.GS_MICRO_CREDITS,
  j.stats:stats:ioRemoteTempWriteBytes / POWER(1024, 3) AS REMOTE_SPILL_GB,
  j.stats:stats:numOriginalRows AS Original_Rows,
  j.stats:stats:producedRows AS Rows_Affected,
  j.stats:stats:returnedRows AS Returned_Rows,
  j.stats:stats:scanOriginalFiles AS Total_Files,
  j.stats:stats:scanFiles AS Files_Scaned,
  CONCAT(
    ROUND(
      (1 - j.stats:stats:scanFiles / j.stats:stats:scanOriginalFiles) * 100,
      2
    ),
    '%'
  ) AS Prune_Rate,
  j.stats:stats:scanBytes AS scanBytes,
  j.stats:stats:returnedRows / j.stats:stats:scanBytes AS QueryEfficiencyRatio,
  jc.TOTAL_CREDITS / j.stats:stats:returnedRows AS CostPerResult,
  j.stats:stats:ioRemoteTempWriteBytes / j.TOTAL_DURATION AS SpilloverImpact,
  j.TOTAL_DURATION - j.DUR_XP_EXECUTING AS ExecutionOverhead,
  j.stats:stats:oomKillCount::number AS oomKillCount,
  j.stats:stats:retryCount::number AS retryCount,
  COALESCE(j.stats:stats:ioRemoteTempWriteBytes::integer, 0) AS bytes_spilled_to_remote_storage,
  j.description AS SQL,
  j.TOTAL_DURATION / 60000 AS TOTAL_MINUTES,
  jc.TOTAL_CREDITS
FROM 
  jobs_with_account j
JOIN 
  snowhouse.product.job_credits jc 
ON 
  jc.deployment = j.DEPLOYMENT AND jc.account_id = j.account_id AND jc.job_uuid = j.query_id
JOIN 
  SNOWHOUSE_IMPORT.PROD.WAREHOUSE_ETL_V wh 
ON 
  j.warehouse_id = wh.id AND j.account_id = wh.account_id;





---***************End of Overall Query Stats************
---Credits per Errors:

select
    j.DEPLOYMENT,
    a.name,
    a.alias,
    j.role_name,
    j.user_name,
    j.database_name,
    j.schema_name,
    j.warehouse_name,
    j.error_code,
    j.error_message,
    COUNT(j.uuid) as TOTAL_QUERIES,
    SUM(j.TOTAL_DURATION/60000 ) as TOTAL_MINUTES,
    SUM(jc.TOTAL_CREDITS) as TOTAL_CREDITS
    
from
    snowhouse_import.prod.job_etl_v j 
    JOIN SNOWHOUSE_IMPORT.PROD.ACCOUNT_ETL_V a ON ( a.deployment = j.deployment AND a.id = j.account_id)
    JOIN snowhouse.product.job_credits jc ON ( jc.deployment = j.deployment AND jc.account_id = j.account_id AND jc.job_uuid = j.uuid)
    
    
where
    1=1
    and j.created_on BETWEEN '2025-06-20 00:00:00' AND '2025-07-06 23:59:59'
    --and j.uuid IN ()
    --and j.ERROR_MESSAGE like '%SQL%execution%'
    --and j.ERROR_CODE = '000630'
    and TOTAL_CREDITS > 0
    and j.ERROR_CODE is not null
    --and a.replication_group = '1003_2ffa4dfd-9b95-4a99-8e82-81abe31d00fe' --Fidelity
    and a.replication_group = '2001_6a16d638-cb22-49e0-9d5a-b3e9f47e041f' --CarMax
    --and a.replication_group = '1015_43de5926-57f9-41c9-8d67-7d2a263e3b21'--Cargill
    --and a.replication_group = '3004_2dfc25fc-4b8d-472c-8541-77fae048b52f' --Solenis
    --and a.replication_group = '2001_6a16d638-cb22-49e0-9d5a-b3e9f47e041f'
    --and a.replication_group = '1003_bedbe564-2f68-4d75-8596-e430ab42cb83' --PIMCO
    Group by 1,2,3,4,5,6,7,8,9,10
    ORDER by TOTAL_CREDITS DESC ;


---Credits by Warehouse
select
    j.DEPLOYMENT,
    a.name,
    a.alias,
    j.role_name,
    j.user_name,
    j.database_name,
    j.schema_name,
    j.warehouse_name,
    wh.warehouse_type,
    wh.size,
    --j.error_code,
    --j.error_message,
    --SUM(j.TOTAL_DURATION/60000 ) as TOTAL_MINUTES,
    COUNT(j.uuid) as TOTAL_QUERIES,
    SUM(jc.TOTAL_CREDITS) as TOTAL_CREDITS
    
from
    snowhouse_import.prod.job_etl_v j 
    JOIN SNOWHOUSE_IMPORT.PROD.ACCOUNT_ETL_V a ON ( a.deployment = j.deployment AND a.id = j.account_id)
    JOIN snowhouse.product.job_credits jc ON ( jc.deployment = j.deployment AND jc.account_id = j.account_id AND jc.job_uuid = j.uuid)
    JOIN SNOWHOUSE_IMPORT.PROD.WAREHOUSE_ETL_V wh ON j.warehouse_id = wh.id AND j.account_id = wh.account_id
where
    1=1
    and j.created_on BETWEEN '2025-06-20 00:00:00' AND '2025-07-06 23:59:59'
    --and j.uuid IN ()
    --and j.ERROR_MESSAGE like '%SQL%execution%'
    --and j.ERROR_CODE = '000630'
    --and j.ERROR_CODE is not null
    --and a.replication_group = '1003_2ffa4dfd-9b95-4a99-8e82-81abe31d00fe'
    --and a.replication_group = '2001_6a16d638-cb22-49e0-9d5a-b3e9f47e041f'
    and a.replication_group = '2001_6a16d638-cb22-49e0-9d5a-b3e9f47e041f' --CarMax
    --and a.replication_group = '1015_43de5926-57f9-41c9-8d67-7d2a263e3b21' -- Cargill
    --and a.replication_group = '3004_2dfc25fc-4b8d-472c-8541-77fae048b52f' --Solenis
    --and a.replication_group = '1003_bedbe564-2f68-4d75-8596-e430ab42cb83' --PIMCO
    --and a.replication_group = '1003_2ffa4dfd-9b95-4a99-8e82-81abe31d00fe' --Fidelity
    Group by 1,2,3,4,5,6,7,8,9,10
    ORDER by TOTAL_CREDITS DESC ;


--SELECT * from FIVETRAN.SALESFORCE.ACCOUNT WHERE NAME = 'Fidelity Investment';


-- ***********
select

    COUNT(DISTINCT j.uuid) as TOTAL_QUERIES,

    
from
    snowhouse_import.prod.job_etl_v j 
    JOIN SNOWHOUSE_IMPORT.PROD.ACCOUNT_ETL_V a ON ( a.deployment = j.deployment AND a.id = j.account_id)
    JOIN snowhouse.product.job_credits jc ON ( jc.deployment = j.deployment AND jc.account_id = j.account_id AND jc.job_uuid = j.uuid)
    JOIN SNOWHOUSE_IMPORT.PROD.WAREHOUSE_ETL_V wh ON (j.warehouse_id = wh.id AND (j.account_id = wh.account_id AND j.deployment = wh.deployment)AND jc.warehouse_id = wh.id)
where
    1=1
    and j.created_on between '2025-06-20 00:00:00' AND '2025-07-06 23:59:59'
    --and j.uuid IN ()
    --and j.ERROR_MESSAGE like '%SQL%execution%'
    --and j.ERROR_CODE = '000630'
    --and j.ERROR_CODE is not null
    --and a.replication_group = '1003_2ffa4dfd-9b95-4a99-8e82-81abe31d00fe'
    --and a.replication_group = '2001_6a16d638-cb22-49e0-9d5a-b3e9f47e041f'
    --and a.replication_group = '1015_43de5926-57f9-41c9-8d67-7d2a263e3b21'; --Cargill
    --and a.replication_group = '3004_2dfc25fc-4b8d-472c-8541-77fae048b52f'; --Solenis


    and a.replication_group = '2001_6a16d638-cb22-49e0-9d5a-b3e9f47e041f'; --CarMax
    --and a.replication_group = '1003_bedbe564-2f68-4d75-8596-e430ab42cb83' --PIMCO
    --Group by 1,2,3,4,5,6,7,8,9,10
    --ORDER by TOTAL_CREDITS DESC ;

select

    COUNT(DISTINCT j.uuid) as TOTAL_QUERIES,

    
from
    snowhouse_import.prod.job_etl_v j 
    JOIN SNOWHOUSE_IMPORT.PROD.ACCOUNT_ETL_V a ON ( a.deployment = j.deployment AND a.id = j.account_id)

where
    1=1
    and j.created_on between '2025-06-20 00:00:00' AND '2025-07-06 23:59:59'
    and j.cloud_services_credits_used > 0 
    --and j.uuid IN ()
    --and j.ERROR_MESSAGE like '%SQL%execution%'
    --and j.ERROR_CODE = '000630'
    --and j.ERROR_CODE is  null
    --and a.replication_group = '1003_2ffa4dfd-9b95-4a99-8e82-81abe31d00fe'
    --and a.replication_group = '2001_6a16d638-cb22-49e0-9d5a-b3e9f47e041f'
    --and a.replication_group = '2001_6a16d638-cb22-49e0-9d5a-b3e9f47e041f'
    and a.replication_group = '2001_6a16d638-cb22-49e0-9d5a-b3e9f47e041f' --CarMax
    and (j.warehouse_id is not null or j.warehouse_external_size is not null); --CarMax
    --and a.replication_group = '1003_bedbe564-2f68-4d75-8596-e430ab42cb83' --PIMCO
    --Group by 1,2,3,4,5,6,7,8,9,10
    --GROUP BY 1,2;
    --ORDER by TOTAL_CREDITS DESC ;

    describe view SNOWHOUSE_IMPORT.PROD.job_etl_v ;

----**********RSA Streamlit app

with job_credits_by_sql_text as (
    select acct.name as account_name
        , job_credits.account_id
        , job_credits.deployment deployment
        , job_etl_v.sql_text_hash as sql_text_hash
        , st.statement_type
        , job_etl_v.stats:stats.warehouseSize as warehouse_size
        , upper(job_etl_v.DPO:"JobDPO:stats":"warehouseExternalSize"::varchar) as warehouse_externalsize
        , sum(job_credits.credits) as total_credits_used_by_sql_text
        , row_number() over (
            partition by job_credits.account_id
                , job_credits.deployment order by total_credits_used_by_sql_text desc
                , warehouse_size desc
                , st.statement_type
                , sql_text_hash
            ) as credits_used_rank
    from snowhouse_import.prod2.account_etl_v acct
        inner join snowhouse_import.prod2.job_etl_v job_etl_v
            on job_etl_v.account_id = acct.id
        join snowhouse.product.job_credits job_credits
            on job_credits.job_uuid = job_etl_v.uuid
            and job_credits.job_id = job_etl_v.job_id
            and job_credits.account_id=job_etl_v.account_id
        join snowhouse.product.statement_type st
            on job_etl_v.statement_properties = st.id
    where acct.id = 298243
        and job_credits.original_start_at >= '2025-05-02'
        and job_credits.original_start_at < '2025-05-09'
        and job_etl_v.created_on >= '2025-05-02'
        and job_etl_v.created_on < '2025-05-09'
        and job_credits.credits > 0
    group by all
)
, job_credits_by_account as (
    select t1.account_name
        , t1.account_id
        , t1.deployment deployment
        , t1.sql_text_hash as sql_text_hash
        , t1.statement_type
        , t1.warehouse_size
        , t1.warehouse_externalsize
        , t1.total_credits_used_by_sql_text as credits_used
        , t1.credits_used_rank
        , min(job_etl_v.created_on) min_created_on
        , max(job_etl_v.created_on) max_created_on
        , count(distinct date_trunc('day',job_etl_v.created_on)) distinct_created_on_date_count
        , any_value(job_etl_v.description) as any_description -- mask this later in a view
        , any_value(job_etl_v.uuid) as any_query_id
        , any_value(job_etl_v.warehouse_name) any_warehouse_name
        , any_value(concat(job_etl_v.database_name,'.',job_etl_v.schema_name)) any_schema_name
        , any_value(job_etl_v.role_name) as any_role_name
        , count(*) query_count
        , avg((job_etl_v.stats:"stats":"scanFiles"/job_etl_v.stats:"stats":"scanOriginalFiles")*100) as avg_partitions_scanned_pct
        , max((job_etl_v.stats:"stats":"scanFiles"/job_etl_v.stats:"stats":"scanOriginalFiles")*100) as max_partitions_scanned_pct
        , avg(job_etl_v.stats:"stats":"scanFiles") as avg_scanned_partitions
        , max(job_etl_v.stats:"stats":"scanFiles") as max_scanned_partitions
        , sum(job_etl_v.stats:"stats":"scanFiles") as sum_scanned_partitions
        , avg(job_etl_v.stats:"stats":"scanOriginalFiles") as avg_total_partitions
        , max(job_etl_v.stats:"stats":"scanOriginalFiles") as max_total_partitions
        , sum(job_etl_v.stats:"stats":"scanOriginalFiles") as sum_total_partitions
        , avg((nvl(job_etl_v.STATS:stats.scanBytes,0))/power(1024,3)) as avg_gb_scanned
        , max((nvl(job_etl_v.STATS:stats.scanBytes,0))/power(1024,3)) as max_gb_scanned
        , sum((nvl(job_etl_v.STATS:stats.scanBytes,0))/power(1024,3)) as sum_gb_scanned
        , avg((nvl(job_etl_v.STATS:stats.ioRemoteTempWriteBytes,0))/power(1024,3)) as avg_gb_spilled_to_remote_storage
        , max((nvl(job_etl_v.STATS:stats.ioRemoteTempWriteBytes,0))/power(1024,3)) as max_gb_spilled_to_remote_storage
        , sum((nvl(job_etl_v.STATS:stats.ioRemoteTempWriteBytes,0))/power(1024,3)) as sum_gb_spilled_to_remote_storage
        , avg((nvl(job_etl_v.STATS:stats.ioLocalTempWriteBytes,0))/power(1024,3)) as avg_gb_spilled_to_local_storage
        , max((nvl(job_etl_v.STATS:stats.ioLocalTempWriteBytes,0))/power(1024,3)) as max_gb_spilled_to_local_storage
        , sum((nvl(job_etl_v.STATS:stats.ioLocalTempWriteBytes,0))/power(1024,3)) as sum_gb_spilled_to_local_storage
        , avg(job_etl_v.stats:stats.serverCount) as avg_server_count
        , avg(100*(job_etl_v.stats:stats.serverCount/job_etl_v.stats:stats.warehouseSize)) as avg_pct_warehouse_used
        , percentile_cont(.90) within group (order by (100*(job_etl_v.stats:stats.serverCount/job_etl_v.stats:stats.warehouseSize))) as p90_warehouse_used
        , avg((dur_queued_load)/1000) as avg_queued_time
        , max((dur_queued_load)/1000) as max_queued_time
        , sum((dur_queued_load)/1000) as sum_queued_time
        , avg((total_duration)/1000) as avg_total_duration
        , max((total_duration)/1000) as max_total_duration
        , sum((total_duration)/1000) as sum_total_duration
        , avg((dur_xp_executing)/1000) as avg_total_execution_time
        , max((dur_xp_executing)/1000) as max_total_execution_time
        , sum((dur_xp_executing)/1000) as sum_total_execution_time
        , avg(100*(job_etl_v.dur_queued_load/nullif(job_etl_v.total_duration,0))) as avg_pct_total_queued
        , max(100*(job_etl_v.dur_queued_load/nullif(job_etl_v.total_duration,0))) as max_pct_total_queued
        , avg(100*(((job_etl_v.STATS:stats.profIdle::number)/( nullif(job_etl_v.stats:serverCount,0)::number * 1000))/job_etl_v.total_duration)) as avg_pcnt_idle_time
        , max(100*(((job_etl_v.STATS:stats.profIdle::number)/( nullif(job_etl_v.stats:serverCount,0)::number * 1000))/job_etl_v.total_duration)) as max_pcnt_idle_time
        , sum((case when job_etl_v.stats:stats.ioRemoteTempWriteBytes is not null then 1 else 0 end)) as num_jobs_spilled_to_remote
        , num_jobs_spilled_to_remote/nullif(query_count,0) as pct_jobs_spilled_to_remote
    from job_credits_by_sql_text t1
        inner join snowhouse.product.statement_type st
            on st.statement_type = t1.statement_type
        inner join snowhouse_import.prod2.job_etl_v job_etl_v
            on job_etl_v.account_id = t1.account_id
            and job_etl_v.sql_text_hash = t1.sql_text_hash
            and job_etl_v.stats:stats.warehouseSize = t1.warehouse_size
            and job_etl_v.statement_properties = st.id
    where job_etl_v.created_on >= '2025-05-02'
        and job_etl_v.created_on < '2025-05-09'
    group by all
)
select any_warehouse_name as warehouse_name
    , warehouse_size
    , warehouse_externalsize
    , query_count::varchar as query_count
    , sql_text_hash::varchar as sql_text_hash
    , credits_used::number(38,4) as credits_used
    , credits_used_rank::number(38,0) as credits_used_rank
    , min_created_on
    , max_created_on
    , substr(any_description,1,300) as sample_query_text_first_300_characters
    , any_query_id::varchar as sample_query_id
    , avg_gb_scanned::varchar as avg_gb_scanned
    , avg_gb_spilled_to_local_storage::varchar as avg_gb_spilled_to_local_storage
    , avg_gb_spilled_to_remote_storage::varchar as avg_gb_spilled_to_remote_storage
    , avg_scanned_partitions::varchar as avg_scanned_partitions
    , avg_total_partitions::varchar as avg_total_partitions
    , avg_partitions_scanned_pct::varchar as avg_partitions_scanned_pct
    , avg_server_count::varchar avg_server_count
    , avg_pct_warehouse_used::varchar as avg_pct_warehouse_used
    , avg_queued_time::varchar as avg_queued_time_seconds
    , avg_total_duration::varchar as avg_total_duration_seconds
    , avg_total_execution_time::varchar as avg_total_execution_time_seconds
    , avg_pct_total_queued::varchar as avg_pct_queued_time
    , avg_pcnt_idle_time::varchar as avg_pcnt_idle_time
from job_credits_by_account
where credits_used_rank is not null
;


SELECT * FROM SNOWHOUSE_IMPORT.PROD.JOB_ETL_V 
WHERE 
ACCOUNT_ID = 192678 
AND DESCRIPTION ILIKE '%VW_VIN_PROFILE_DATA%'; 



select
    j.DEPLOYMENT,
    a.name,
    a.alias,
    j.role_name,
    j.user_name,
    j.database_name,
    j.schema_name,
    j.warehouse_name,
    wh.warehouse_type,
    wh.size,
    j.uuid,
    j.description,
    j.error_code,
    j.error_message,
    SUM(j.TOTAL_DURATION/60000 ) as TOTAL_MINUTES,
    COUNT(j.uuid) as TOTAL_QUERIES,
    SUM(jc.TOTAL_CREDITS) as TOTAL_CREDITS
    
from
    snowhouse_import.prod.job_etl_v j 
    JOIN SNOWHOUSE_IMPORT.PROD.ACCOUNT_ETL_V a ON ( a.deployment = j.deployment AND a.id = j.account_id)
    JOIN snowhouse.product.job_credits jc ON ( jc.deployment = j.deployment AND jc.account_id = j.account_id AND jc.job_uuid = j.uuid)
    JOIN SNOWHOUSE_IMPORT.PROD.WAREHOUSE_ETL_V wh ON j.warehouse_id = wh.id AND j.account_id = wh.account_id
where j.uuid IN('01bc9dde-0000-1f92-000e-ee8b00028016','01bc9dd8-0000-1f89-000e-ee8b0002506a',
'01bc9dd6-0000-1f52-000e-ee8b00018786','01bc9dd8-0000-1f1b-000e-ee8b0002003e',
'01bc9ddf-0000-1f8a-000e-ee8b0002305e','01bc9dda-0000-1f83-000e-ee8b00022052',
'01bc9dda-0000-1f8d-000e-ee8b00026032','01bc9dde-0000-1f88-000e-ee8b00024046',
'01bc9dd7-0000-1f89-000e-ee8b00025066','01bc9dd8-0000-1f92-000e-ee8b00028006',
'01bc9de4-0000-1f7a-000e-ee8b0001f4b2','01bc9de0-0000-1f7a-000e-ee8b0001f4a2',
'01bc9de4-0000-1f8d-000e-ee8b00026096')
GROUP BY ALL;

select
    j.DEPLOYMENT,
    a.name,
    a.alias,
    j.role_name,
    j.user_name,
    j.database_name,
    j.schema_name,
    j.warehouse_name,
    wh.warehouse_type,
    wh.size,
    j.uuid,
    j.description,
    j.error_code,
    j.error_message,
    SUM(j.TOTAL_DURATION/60000 ) as TOTAL_MINUTES,
    COUNT(j.uuid) as TOTAL_QUERIES,
    SUM(jc.TOTAL_CREDITS) as TOTAL_CREDITS
    
from
    snowhouse_import.prod.job_etl_v j 
    JOIN SNOWHOUSE_IMPORT.PROD.ACCOUNT_ETL_V a ON ( a.deployment = j.deployment AND a.id = j.account_id)
    JOIN snowhouse.product.job_credits jc ON ( jc.deployment = j.deployment AND jc.account_id = j.account_id AND jc.job_uuid = j.uuid)
    JOIN SNOWHOUSE_IMPORT.PROD.WAREHOUSE_ETL_V wh ON j.warehouse_id = wh.id AND j.account_id = wh.account_id
where j.uuid IN('01bc91a4-0000-1f1c-0000-000eee8bd22d','01bc91cb-0000-1f25-0000-000eee8bf231',
'01bc9220-0000-1f25-0000-000eee8bf479','01bc91c0-0000-1f25-0000-000eee8bf201',
'01bc91d4-0000-1f25-0000-000eee8bf26d','01bc91bd-0000-1f25-0000-000eee8bf1f9',
'01bc91b3-0000-1f25-0000-000eee8bf1bd','01bc91eb-0000-1f25-0000-000eee8bf379',
'01bc91dc-0000-1f1c-0000-000eee8bd3f5','01bc91a9-0000-1f1c-0000-000eee8bd275',
'01bc91aa-0000-1f25-0000-000eee8bf171','01bca7e6-0000-1fd5-000e-ee8b0004b7be',
'01bca7d5-0000-1feb-000e-ee8b00052c0a','01bca7d8-0000-1feb-000e-ee8b00052c72',
'01bcac8b-0000-1fc7-000e-ee8b00055616','01bcac99-0000-1fd5-000e-ee8b000582ce',
'01bcca2c-0000-2013-000e-ee8b0006d682','01bcca33-0000-1fd5-000e-ee8b0006e396')
GROUP BY ALL;

select
    j.DEPLOYMENT,
    a.name,
    a.alias,
    j.role_name,
    j.user_name,
    j.database_name,
    j.schema_name,
    j.warehouse_name,
    wh.warehouse_type,
    wh.size,
    j.uuid,
    j.description,
    j.error_code,
    j.error_message,
    SUM(j.TOTAL_DURATION/60000 ) as TOTAL_MINUTES,
    COUNT(j.uuid) as TOTAL_QUERIES,
    SUM(jc.TOTAL_CREDITS) as TOTAL_CREDITS
    
from
    snowhouse_import.prod.job_etl_v j 
    JOIN SNOWHOUSE_IMPORT.PROD.ACCOUNT_ETL_V a ON ( a.deployment = j.deployment AND a.id = j.account_id)
    JOIN snowhouse.product.job_credits jc ON ( jc.deployment = j.deployment AND jc.account_id = j.account_id AND jc.job_uuid = j.uuid)
    JOIN SNOWHOUSE_IMPORT.PROD.WAREHOUSE_ETL_V wh ON j.warehouse_id = wh.id AND j.account_id = wh.account_id
where j.uuid IN('01bc9c12-0000-1f50-000e-ee8b0001c0d2','01bc9c1a-0000-1f50-000e-ee8b0001c106',
'01bc9c12-0000-1f52-000e-ee8b000181e2','01bc9c72-0000-1f52-000e-ee8b000182ca',
'01bc9c0d-0000-1f52-000e-ee8b000181ba','01bc9c23-0000-1f4e-000e-ee8b0001a1e6',
'01bc9c21-0000-1f4e-000e-ee8b0001a1ce','01bc9c6f-0000-1f52-000e-ee8b000182a2',
'01bc9c10-0000-1f52-000e-ee8b000181d6','01bcaca0-0000-1fd5-000e-ee8b0005836e',
'01bcaca3-0000-1fd5-000e-ee8b000583ae','01bcaca3-0000-1fc7-000e-ee8b00055756',
'01bcaca1-0000-1fd5-000e-ee8b0005837a','01bcaca2-0000-1fc7-000e-ee8b00055752')
GROUP BY ALL;



--***************Befor and after query comp
select
    j.DEPLOYMENT,
    a.name,
    a.alias,
    DATE(j.created_on),
    COUNT(j.uuid) as TOTAL_QUERIES,
    SUM(j.TOTAL_DURATION/60000 ) as TOTAL_MINUTES,
    SUM(jc.TOTAL_CREDITS) as TOTAL_CREDITS
    
from
    snowhouse_import.prod.job_etl_v j 
    JOIN SNOWHOUSE_IMPORT.PROD.ACCOUNT_ETL_V a ON ( a.deployment = j.deployment AND a.id = j.account_id)
    JOIN snowhouse.product.job_credits jc ON ( jc.deployment = j.deployment AND jc.account_id = j.account_id AND jc.job_uuid = j.uuid)
    
    
where
    1=1
    and j.created_on BETWEEN '2025-06-20 00:00:00' AND '2025-07-06 23:59:59'
    --and j.uuid IN ()
    --and j.ERROR_MESSAGE like '%SQL%execution%'
    --and j.ERROR_CODE = '000630'
    and TOTAL_CREDITS > 0
    --and j.ERROR_CODE is not null
    --and a.replication_group = '1003_2ffa4dfd-9b95-4a99-8e82-81abe31d00fe' --Fidelity
    and a.replication_group = '2001_6a16d638-cb22-49e0-9d5a-b3e9f47e041f' --CarMax
    --and a.replication_group = '1015_43de5926-57f9-41c9-8d67-7d2a263e3b21'--Cargill
    --and a.replication_group = '3004_2dfc25fc-4b8d-472c-8541-77fae048b52f' --Solenis
    --and a.replication_group = '2001_6a16d638-cb22-49e0-9d5a-b3e9f47e041f'
    --and a.replication_group = '1003_bedbe564-2f68-4d75-8596-e430ab42cb83' --PIMCO
    Group by ALL
    ORDER by TOTAL_CREDITS DESC ;

---LRQ Results ------

WITH 
  -- Filter job_etl_v table first to reduce the data volume
  filtered_jobs AS (
    SELECT 
      DEPLOYMENT,
      role_name,
      user_name,
      database_name,
      schema_name,
      warehouse_name,
      warehouse_id,
      account_id,
      QUERY_PARAMETERIZED_HASH,
      SQL_TEXT_HASH,
      uuid,
      CREATED_ON,
      dur_compiling,
      DUR_XP_EXECUTING,
      error_code,
      error_message,
      CLOUD_SERVICES_CREDITS_USED,
      stats,
      description,
      TOTAL_DURATION
    FROM 
      snowhouse_import.prod.job_etl_v
    WHERE 
      CREATED_ON BETWEEN '2025-07-09 00:00:00' AND '2025-07-16 23:59:59'
      --and (TOTAL_DURATION/ 60000) > 30
  ),
  -- Join filtered_jobs with account_etl_v
  jobs_with_account AS (
    SELECT 
      j.DEPLOYMENT,
      a.name,
      a.alias,
      j.role_name,
      j.user_name,
      j.database_name,
      j.schema_name,
      j.warehouse_name,
      j.warehouse_id,
      j.account_id,
      j.QUERY_PARAMETERIZED_HASH AS sql_hash,
      j.SQL_TEXT_HASH,
      j.uuid AS query_id,
      j.CREATED_ON AS RUN_DATE,
      j.dur_compiling / 1000::number AS COMPILING_SECONDS,
      j.DUR_XP_EXECUTING / 60000 AS EXECUTING_MINUTES,
      j.error_code,
      j.error_message,
      j.CLOUD_SERVICES_CREDITS_USED,
      j.stats,
      j.description,
      J.DUR_XP_EXECUTING,
      j.TOTAL_DURATION
    FROM 
      filtered_jobs j
    JOIN 
      SNOWHOUSE_IMPORT.PROD.ACCOUNT_ETL_V a 
    ON 
      a.deployment = j.deployment AND a.id = j.account_id
    WHERE 
      --a.replication_group = '1003_2ffa4dfd-9b95-4a99-8e82-81abe31d00fe'
      a.replication_group = '2001_6a16d638-cb22-49e0-9d5a-b3e9f47e041f' --CarMax
      and a.alias = 'PROD1'
      --a.replication_group = '1015_43de5926-57f9-41c9-8d67-7d2a263e3b21' -- Cargill
      --a.replication_group = '3004_2dfc25fc-4b8d-472c-8541-77fae048b52f' --Solenis
      ----a.replication_group = '1003_2ffa4dfd-9b95-4a99-8e82-81abe31d00fe' --Fidelity
      --a.replication_group = '1003_bedbe564-2f68-4d75-8596-e430ab42cb83' --PIMCO
      --and j.ERROR_CODE is not null
  )

SELECT 
  --j.DEPLOYMENT,
  --j.name,
  --j.alias,
  --j.role_name,
  --j.user_name,
  --j.database_name,
  --j.schema_name,
  j.warehouse_name,
  --wh.warehouse_type,
  --wh.size,
  --j.stats:stats:warehouseSize::number AS wh_sz_Number,
  --AVG(j.stats:stats:totalMemory::number) AS total_memory,
  --j.stats:stats:serverCount::number AS server_count,
  --j.sql_hash,
  --TO_VARCHAR(j.SQL_TEXT_HASH),
  --j.query_id,
  --j.RUN_DATE,
  --j.COMPILING_SECONDS,
  --j.EXECUTING_MINUTES,
  --j.error_code,
  --j.error_message,
  --j.CLOUD_SERVICES_CREDITS_USED,
  --jc.CREDITS,
  --jc.XP_CREDITS,
  --jc.GS_CREDITS,
  --jc.XP_MICRO_CREDITS,
  --jc.GS_MICRO_CREDITS,
  --j.stats:stats:ioRemoteTempWriteBytes / POWER(1024, 3) AS REMOTE_SPILL_GB,
  --j.stats:stats:numOriginalRows AS Original_Rows,
  --j.stats:stats:producedRows AS Rows_Affected,
  --j.stats:stats:returnedRows AS Returned_Rows,
  --j.stats:stats:scanOriginalFiles AS Total_Files,
  --j.stats:stats:scanFiles AS Files_Scaned,
  -- CONCAT(
  --   ROUND(
  --     (1 - j.stats:stats:scanFiles / j.stats:stats:scanOriginalFiles) * 100,
  --     2
  --   ),
  --   '%'
  -- ) AS Prune_Rate,
  -- j.stats:stats:scanBytes AS scanBytes,
  -- j.stats:stats:returnedRows / j.stats:stats:scanBytes AS QueryEfficiencyRatio,
  -- jc.TOTAL_CREDITS / j.stats:stats:returnedRows AS CostPerResult,
  -- j.stats:stats:ioRemoteTempWriteBytes / j.TOTAL_DURATION AS SpilloverImpact,
  -- j.TOTAL_DURATION - j.DUR_XP_EXECUTING AS ExecutionOverhead,
  -- j.stats:stats:oomKillCount::number AS oomKillCount,
  -- j.stats:stats:retryCount::number AS retryCount,
  -- COALESCE(j.stats:stats:ioRemoteTempWriteBytes::integer, 0) AS bytes_spilled_to_remote_storage,
  -- j.description AS SQL,
  -- j.TOTAL_DURATION / 60000 AS TOTAL_MINUTES,
  -- jc.TOTAL_CREDITS
FROM 
  jobs_with_account j
JOIN 
  snowhouse.product.job_credits jc 
ON 
  jc.deployment = j.DEPLOYMENT AND jc.account_id = j.account_id AND jc.job_uuid = j.query_id
JOIN 
  SNOWHOUSE_IMPORT.PROD.WAREHOUSE_ETL_V wh 
ON 
  j.warehouse_id = wh.id AND j.account_id = wh.account_id
  Group by all;
