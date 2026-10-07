# Project Documentation

The documentation is published at **https://snowflake-labs.github.io/pg_lake/**.

**Get started**
- [Get started](./get-started.md)
- [Building from source](./building-from-source.md)
- [Configuration](./configuration.md)
- [How pg_lake works](./concepts.md)

**User guide**
- [Iceberg tables](./iceberg-tables.md): [partitioning](./iceberg-partitioning.md),
  [modifying tables](./iceberg-modifying.md), [catalogs and interoperability](./iceberg-catalogs.md),
  [maintenance](./iceberg-maintenance.md)
- [Query data lake files](./query-data-lake-files.md)
- [Import and export](./data-lake-import-export.md)
- [Geospatial](./spatial.md)
- [Performance](./performance.md)
- [dbt](./dbt.md)

**Use cases**
- [Sync Postgres tables to Iceberg](./use-case-iceberg-sync.md)
- [Log management](./use-case-log-management.md)
- [Archive partitions to Iceberg](./use-case-archiving.md)
- [Fast analytics dashboards](./use-case-dashboards.md)
- [Geospatial analytics](./use-case-geospatial.md)

**Reference**
- [SQL functions and views](./sql-reference.md)
- [Table options](./table-options.md)
- [Configuration parameters](./settings-reference.md)
- [Data types](./data-types.md)
- [File formats](./file-formats-reference.md)

For the main project overview, see the [root README](../README.md).

## Working on the docs site

The [Publish documentation](../.github/workflows/docs.yml) workflow builds the site from this
directory on every push to `main` that changes it, and deploys the result to the `gh-pages`
branch. GitHub Pages serves the site from that branch. Pull requests that touch `docs/` run the
same build without deploying. The site uses the
[Just the Docs](https://just-the-docs.com/) theme configured in `_config.yml`. Each page's
front matter sets its title and place in the sidebar (`parent`, `grand_parent`, `nav_order`), so
a new page needs a front matter block to show up in the navigation. Please also add new pages
to the list above.

Pages are processed by Liquid, so wrap code blocks that contain `{{ }}` or `{% %}` (for example
dbt models) in `{% raw %}` and `{% endraw %}` tags, as in `dbt.md`.

To preview the site locally with Ruby and Bundler:

```bash
cd docs
cat > Gemfile <<'EOF'
source "https://rubygems.org"
gem "github-pages", group: :jekyll_plugins
EOF
bundle install
bundle exec jekyll serve
```
