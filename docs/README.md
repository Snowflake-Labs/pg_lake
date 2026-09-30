# Project Documentation

The documentation is published at **https://snowflake-labs.github.io/pg_lake/**.

- [**Get started**](./get-started.md)
- [**Building from Source**](./building-from-source.md)  
- [**Iceberg Tables**](./iceberg-tables.md)  
- [**Query Data Lake Files**](./query-data-lake-files.md)  
- [**Data Lake Import & Export**](./data-lake-import-export.md)  
- [**File Formats Reference**](./file-formats-reference.md) 
- [**Geospatial features**](./spatial.md) 
- [**DBT Integration**](./dbt.md)  
- [**Use Case: Log Management**](./use-case-log-management.md)  

For the main project overview, see the [root README](../README.md).

## Working on the docs site

GitHub Pages builds the site from this directory on every push to `main`, using the
[Just the Docs](https://just-the-docs.com/) theme configured in `_config.yml`. Each page's
front matter sets its title and place in the sidebar (`parent`, `nav_order`), so a new page
needs a front matter block to show up in the navigation.

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
