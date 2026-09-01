# dbt-doris

This is the doris adapter plugin for dbt.

## Install

```shell
git clone https://github.com/apache/doris.git
cd doris/extension/dbt-doris && pip install .
```

## Status

This adapter currently targets the dbt Core v1 Python runtime. It is not yet a dbt Fusion adapter. For the migration roadmap and implementation plan, see [docs/dbt-doris-v1-v2-evaluation.zh-CN.md](docs/dbt-doris-v1-v2-evaluation.zh-CN.md).

## Configuring your profile

Example entry for profiles.yml:

```yaml
your_profile_name:
  target: dev
  outputs:
    dev:
      type: doris
      host: 127.0.0.1
      port: 9030
      username: root
      schema: dbt
```

## Catalog-qualified relations

The adapter supports both Doris internal-catalog relations and external-catalog
relations. Omit `database` in the profile for the internal catalog; `schema` is
the Doris database. A source or model that sets `database` to a Doris catalog
renders a three-part relation:

```yaml
sources:
  - name: hive_orders
    database: hive_catalog
    schema: ods
    tables:
      - name: orders
```

```sql
select * from `hive_catalog`.`ods`.`orders`
```

`database` and `schema` are independent relation components. External catalogs
must already exist in Doris and must provide the permissions required by the
operation; the adapter does not create or configure external catalog
connections.

Projects that previously repeated the same Doris database in both `database`
and `schema` should remove `database` for Internal Catalog objects.
