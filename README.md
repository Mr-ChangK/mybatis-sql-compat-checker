# mybatis-sql-compat-checker

Lightweight Maven plugin that validates MyBatis mapper SQL against different databases. It scans mapper XMLs, renders JDBC-ready SQL, and runs `EXPLAIN (FORMAT JSON)` or executes the statements inside a rolled-back transaction to catch Oracle→Postgres incompatibilities early.

## Why
- Oracle and PostgreSQL have subtle syntax and type differences; handwritten mapper SQL is easy to overlook.
- This plugin surfaces breakage during the build, not after deploys.
- Generates a machine-readable report for CI pipelines.

## Features (current)
- Scans mapper XML files with include/exclude patterns.
- Captures namespace-qualified statement ids and parameters.
- Binds sample values per `jdbcType` to validate parameter binding.
- Validates via `EXPLAIN (FORMAT JSON)` by default; can optionally execute inside savepoints.
- Emits a JSON report to `target/sql-valid-report.json` with per-statement status.

## Quickstart
```bash
mvn io.github.chedwick:mybatis-sql-compat-checker:0.1.0-SNAPSHOT:validate-sql \
  -DvalidateSql.jdbcUrl=jdbc:postgresql://localhost:5432/yourdb \
  -DvalidateSql.username=youruser \
  -DvalidateSql.password=secret
```

Key flags:
- `-DvalidateSql.executeStatements=true` to actually run statements (still rolled back).
- `-DvalidateSql.mapperDirectories=src/main/resources,src/test/resources` to override locations.
- `-DvalidateSql.includes=**/*Mapper.xml` and `-DvalidateSql.excludes=**/legacy/**` for fine control.
- `-DvalidateSql.reportPath=target/sql-valid-report.json` to relocate the report.
- `-DvalidateSql.originJdbcUrl=...` (plus username/password) to validate origin DB first; target DB runs only if origin passes.
