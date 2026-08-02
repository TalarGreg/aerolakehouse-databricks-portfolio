# Databricks SQL DDL

These scripts manage data-object definitions that are intentionally outside
Terraform. Run them in numeric order when their prerequisites are available.

| Script | When to run | Objects |
|---|---|---|
| `010_create_monitoring_ingestion_audit.sql` | Before the first pipeline run | Monitoring audit table |
| `030_create_our_airports_bronze_tables.sql` | Before the first pipeline run | Four `COPY INTO` target tables |
| `040_set_bronze_tblproperties.sql` | After the first successful pipeline run | Optimization properties for dynamically created append tables |

Each script defaults `target_catalog` to `dev_alh` and uses `IDENTIFIER(...)`
to construct object names safely. For another environment, change only the
variable value at the top of the script.

The `CREATE TABLE IF NOT EXISTS` statements are safe to run repeatedly, but
they do not update an existing table definition. Add a new numbered migration
using `ALTER TABLE` for future schema changes instead of editing an already
applied migration.
