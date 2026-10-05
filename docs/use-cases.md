---
title: Use cases
nav_order: 5
has_children: true
has_toc: false
---

# Use cases

End-to-end examples of pg_lake applied to real workloads. Each one can be run as written on a
pg_lake installation with object storage.

<div class="pglake-cards">
  <a class="pglake-card" href="{{ '/use-case-iceberg-sync.html' | relative_url }}">
    <span class="pglake-card-title">Sync Postgres tables to Iceberg</span>
    <span class="pglake-card-text">Keep an Iceberg copy of operational tables up to date with pg_incremental, and query it from any Iceberg engine without ETL.</span>
  </a>
  <a class="pglake-card" href="{{ '/use-case-log-management.html' | relative_url }}">
    <span class="pglake-card-title">Log management</span>
    <span class="pglake-card-text">Turn log files in object storage into a compact Iceberg table, processing each new file exactly once.</span>
  </a>
  <a class="pglake-card" href="{{ '/use-case-archiving.html' | relative_url }}">
    <span class="pglake-card-title">Archive partitions to Iceberg</span>
    <span class="pglake-card-text">Keep recent rows in heap partitions and move old months into a partitioned Iceberg table.</span>
  </a>
  <a class="pglake-card" href="{{ '/use-case-dashboards.html' | relative_url }}">
    <span class="pglake-card-title">Fast analytics dashboards</span>
    <span class="pglake-card-text">Serve dashboards from Iceberg tables with sub-second aggregates over millions of rows, and keep rollups up to date for busy panels.</span>
  </a>
  <a class="pglake-card" href="{{ '/use-case-geospatial.html' | relative_url }}">
    <span class="pglake-card-title">Geospatial analytics</span>
    <span class="pglake-card-text">Query public GeoParquet and Shapefiles in place, extract them into Iceberg and PostGIS tables, run spatial joins and map the results in QGIS.</span>
  </a>
</div>
