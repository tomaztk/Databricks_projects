CREATE SCHEMA IF NOT EXISTS hive_metastore.demo;

CREATE OR REPLACE TABLE hive_metastore.demo.hello_uc (
  id   INT,
  name STRING
) COMMENT 'Seeded by GitHub Actions pipeline';

INSERT INTO hive_metastore.demo.hello_uc (id, name) VALUES
 (1,'one'),
 (2,'two'),
 (3,'three'),
 (4,'four'),
 (5,'five'),
 (6,'six'),
 (7,'seven'),
 (8,'eight'),
 (9,'nine'),
 (10,'ten');
