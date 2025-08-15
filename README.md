# Databricks projects


## UC via Github


The workflow uses the Databricks SQL CLI (dbsqlcli) in non-interactive mode to execute a .sql file directly from the cloned repo via -e sql/seed_uc.sql. The CLI explicitly supports running a query string or a file; we also create a minimal ~/.dbsqlcli/dbsqlclirc file because the tool expects it to exist.

The SQL itself uses standard Unity Catalog object naming (catalog.schema.table) and current CREATE TABLE syntax for Databricks SQL.

You obtain the host and HTTP Path from your SQL Warehouse’s connection details (these are exactly what dbsqlcli needs).
