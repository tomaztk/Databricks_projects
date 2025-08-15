CREATE SCHEMA IF NOT EXISTS hive_metastore.demo;

CREATE OR REPLACE TABLE hive_metastore.demo.hello_github_uc (
   id  INT ,
   name STRING DEFAULT 'Hello UC from Github',
   Insert_date TIMESTAMP  DEFAULT CURRENT_TIMESTAMP(),
   year INT GENERATED ALWAYS AS (YEAR(Insert_date))
) COMMENT 'Seeded by GitHub Actions pipeline'
TBLPROPERTIES (
'delta.feature.allowColumnDefaults' = 'supported', 
'delta.columnMapping.mode' = 'name',
'delta.enableDeletionVectors' = true,
'delta.minReaderVersion' = '1', 
'delta.minWriterVersion'='7');

INSERT INTO hive_metastore.demo.hello_github_uc(id)
SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4