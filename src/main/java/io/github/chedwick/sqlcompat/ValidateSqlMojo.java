package io.github.chedwick.sqlcompat;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Savepoint;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.LinkedHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.sql.DataSource;

/**
 * Validates MyBatis mapper SQL compatibility against PostgreSQL by running EXPLAIN or
 * executing statements inside a rolled-back transaction.
 */
@Mojo(name = "validate-sql", defaultPhase = LifecyclePhase.VERIFY, threadSafe = true)
public class ValidateSqlMojo extends AbstractMojo {

    @Parameter(property = "validateSql.jdbcUrl", required = true)
    private String jdbcUrl;

    @Parameter(property = "validateSql.username")
    private String username;

    @Parameter(property = "validateSql.password")
    private String password;

    @Parameter(property = "validateSql.originJdbcUrl")
    private String originJdbcUrl;

    @Parameter(property = "validateSql.originUsername")
    private String originUsername;

    @Parameter(property = "validateSql.originPassword")
    private String originPassword;

    @Parameter(property = "validateSql.mapperDirectories")
    private List<String> mapperDirectories = Arrays.asList("src/main/resources");

    @Parameter(property = "validateSql.includes")
    private List<String> includes = Arrays.asList("**/*Mapper.xml");

    @Parameter(property = "validateSql.excludes")
    private List<String> excludes = new ArrayList<>();

    @Parameter(property = "validateSql.statementTimeoutSeconds", defaultValue = "8")
    private int statementTimeoutSeconds;

    @Parameter(property = "validateSql.reportPath", defaultValue = "${project.build.directory}/sql-valid-report.json")
    private String reportPath;

    /**
     * When true, execute the statements; otherwise run EXPLAIN which avoids writes.
     */
    @Parameter(property = "validateSql.executeStatements", defaultValue = "false")
    private boolean executeStatements;

    @Parameter(property = "validateSql.threadCount", defaultValue = "4")
    private int threadCount;

    @Override
    public void execute() throws MojoExecutionException {
        Objects.requireNonNull(jdbcUrl, "jdbcUrl");
        Objects.requireNonNull(originJdbcUrl, "originJdbcUrl");
        MapperScanner scanner = new MapperScanner(getLog(), mapperDirectories, includes, excludes);
        List<SqlStatement> statements;
        try {
            statements = scanner.scan();
        } catch (Exception e) {
            throw new MojoExecutionException("Failed to scan mapper XML files", e);
        }
        if (statements.isEmpty()) {
            getLog().warn("No mapper statements found. Check mapperDirectories/includes/excludes.");
            return;
        }

        getLog().info("Found " + statements.size() + " mapped statements. Validating...");
        List<ValidationResult> results = new ArrayList<>();
        ValidationSummary originSummary = validateDatabase("origin", originJdbcUrl, originUsername, originPassword, statements, results);
        ValidationSummary targetSummary = null;
        if (originSummary.failures == 0) {
            targetSummary = validateDatabase("target", jdbcUrl, username, password, statements, results);
        } else {
            getLog().warn("Skipping target database validation because origin database had failures.");
        }

        long total = results.size();
        long failures = results.stream().filter(r -> !r.success).count();
        try {
            writeReport(results, originSummary, targetSummary);
        } catch (Exception e) {
            getLog().warn("Failed to write report: " + e.getMessage());
        }
        getLog().info("Validation summary:");
        getLog().info(" - origin: " + originSummary.failures + " failure(s) out of " + originSummary.total);
        if (targetSummary != null) {
            getLog().info(" - target: " + targetSummary.failures + " failure(s) out of " + targetSummary.total);
        } else {
            getLog().info(" - target: skipped");
        }
        for (ValidationResult result : results) {
            if (result.success) {
                getLog().info("OK   " + result.statement.fullId() + " (" + result.statement.kind() + ") [" + result.databaseLabel + "]");
            } else {
                getLog().error("FAIL " + result.statement.fullId() + " (" + result.statement.kind() + ") [" + result.databaseLabel + "] " + result.errorMessage);
            }
        }
        if (failures > 0) {
            throw new MojoExecutionException("Validation failed for " + failures + " statement(s)");
        }
        StringBuilder successMsg = new StringBuilder();
        successMsg.append("SQL compatibility validation passed for ").append(total).append(" statement(s)");
        if (reportPath != null && !reportPath.isBlank()) {
            try {
                successMsg.append(". Report: ").append(Path.of(reportPath).toAbsolutePath());
            } catch (Exception ignored) {
                // Path may be invalid; avoid failing success case
            }
        }
        getLog().info(successMsg.toString());
    }

    private ValidationSummary validateDatabase(String label,
                                               String url,
                                               String user,
                                               String pass,
                                               List<SqlStatement> statements,
                                               List<ValidationResult> collector) throws MojoExecutionException {
        getLog().info("Validating against " + label + " database: " + url + " with " + Math.max(1, threadCount) + " thread(s)");
        DataSource dataSource = createDataSource(url, user, pass);
        ExecutorService executor = Executors.newFixedThreadPool(Math.max(1, threadCount));
        int failures = 0;
        try {
            List<Future<ValidationResult>> futures = new ArrayList<>();
            for (SqlStatement stmt : statements) {
                futures.add(executor.submit((Callable<ValidationResult>) () -> validateWithDataSource(label, dataSource, stmt)));
            }

            for (Future<ValidationResult> future : futures) {
                try {
                    ValidationResult result = future.get();
                    collector.add(result);
                    if (!result.success) {
                        failures++;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new MojoExecutionException("Validation interrupted for " + label, e);
                } catch (ExecutionException e) {
                    failures++;
                    collector.add(ValidationResult.failure(new SqlStatement("<unknown>", label, SqlStatement.Kind.UNKNOWN, Path.of("<n/a>"), "", List.of()), label, label + ": " + e.getCause().getMessage()));
                }
            }
        } finally {
            executor.shutdownNow();
            if (dataSource instanceof AutoCloseable closeable) {
                try {
                    closeable.close();
                } catch (Exception ignored) {
                    // best effort
                }
            }
        }
        return new ValidationSummary(label, statements.size(), failures);
    }

    private ValidationResult validateWithDataSource(String dbLabel,
                                                    DataSource dataSource,
                                                    SqlStatement stmt) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            if (!executeStatements) {
                conn.setReadOnly(true);
            }
            ValidationResult result = validateStatement(conn, stmt, dbLabel);
            try {
                conn.rollback();
            } catch (SQLException ignored) {
                // ignore rollback issues on close
            }
            return result;
        } catch (Exception e) {
            return ValidationResult.failure(stmt, dbLabel, dbLabel + ": " + e.getMessage());
        }
    }

    private ValidationResult validateStatement(Connection conn, SqlStatement stmt, String dbLabel) {
        String preparedSql = toPreparedSql(stmt.rawSql());
        String sqlToRun = executeStatements ? preparedSql : "EXPLAIN (FORMAT JSON) " + preparedSql;
        ParameterGenerator generator = new ParameterGenerator();
        Savepoint sp = null;
        try {
            sp = conn.setSavepoint("sql_valid");
            try (PreparedStatement ps = conn.prepareStatement(sqlToRun)) {
                ps.setQueryTimeout(statementTimeoutSeconds);
                generator.bind(ps, stmt.parameters());
                ps.execute();
            }
            if (sp != null) {
                conn.rollback(sp);
            }
            return ValidationResult.success(stmt, dbLabel);
        } catch (Exception e) {
            try {
                if (sp != null) {
                    conn.rollback(sp);
                }
            } catch (SQLException ignored) {
                // ignore rollback problems
            }
            return ValidationResult.failure(stmt, dbLabel, dbLabel + ": " + e.getMessage());
        }
    }

    private static String toPreparedSql(String myBatisSql) {
        if (myBatisSql == null) {
            return "";
        }
        // Replace #{...} with ? to use JDBC parameters.
        return Pattern.compile("#\\{[^}]+}").matcher(myBatisSql).replaceAll("?");
    }

    private void writeReport(List<ValidationResult> results, ValidationSummary origin, ValidationSummary target) throws Exception {
        if (reportPath == null || reportPath.isBlank()) {
            return;
        }
        Path out = Path.of(reportPath);
        Path parent = out.getParent();
        if (parent != null) {
            java.nio.file.Files.createDirectories(parent);
        }
        StringBuilder sb = new StringBuilder();
        sb.append("{\"total\":").append(results.size()).append(",");
        long failures = results.stream().filter(r -> !r.success).count();
        sb.append("\"failures\":").append(failures).append(",");
        sb.append("\"databases\":[");
        sb.append(origin.toJson());
        if (target != null) {
            sb.append(",").append(target.toJson());
        }
        sb.append("],");
        sb.append("\"entriesByDatabase\":{");
        Map<String, List<ValidationResult>> byDb = groupByDatabase(results);
        int dbIdx = 0;
        for (Map.Entry<String, List<ValidationResult>> entry : byDb.entrySet()) {
            sb.append("\"").append(escape(entry.getKey())).append("\":");
            writeEntriesArray(sb, entry.getValue());
            if (dbIdx < byDb.size() - 1) {
                sb.append(",");
            }
            dbIdx++;
        }
        sb.append("}}");
        java.nio.file.Files.writeString(out, sb.toString());
        getLog().info("Wrote validation report to " + out.toAbsolutePath());
    }

    private static Map<String, List<ValidationResult>> groupByDatabase(List<ValidationResult> results) {
        Map<String, List<ValidationResult>> byDb = new LinkedHashMap<>();
        for (ValidationResult r : results) {
            byDb.computeIfAbsent(r.databaseLabel, k -> new ArrayList<>()).add(r);
        }
        return byDb;
    }

    private static void writeEntriesArray(StringBuilder sb, List<ValidationResult> results) {
        sb.append("[");
        for (int i = 0; i < results.size(); i++) {
            ValidationResult r = results.get(i);
            sb.append("{\"id\":\"").append(escape(r.statement.fullId())).append("\",");
            sb.append("\"kind\":\"").append(r.statement.kind()).append("\",");
            sb.append("\"file\":\"").append(escape(r.statement.sourceFile().toString())).append("\",");
            sb.append("\"database\":\"").append(escape(r.databaseLabel)).append("\",");
            sb.append("\"success\":").append(r.success).append(",");
            sb.append("\"error\":");
            if (r.errorMessage == null) {
                sb.append("null");
            } else {
                sb.append("\"").append(escape(r.errorMessage)).append("\"");
            }
            sb.append("}");
            if (i < results.size() - 1) {
                sb.append(",");
            }
        }
        sb.append("]");
    }

    private static String escape(String raw) {
        StringBuilder sb = new StringBuilder();
        for (char c : raw.toCharArray()) {
            switch (c) {
                case '\\' -> sb.append("\\\\");
                case '"' -> sb.append("\\\"");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }

    DataSource createDataSource(String url, String user, String pass) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        if (user != null) {
            config.setUsername(user);
        }
        if (pass != null) {
            config.setPassword(pass);
        }
        int threads = Math.max(1, threadCount);
        config.setMaximumPoolSize(threads);
        config.setMinimumIdle(0);
        config.setAutoCommit(false);
        config.setPoolName("sql-valid-" + Math.abs(url.hashCode()));
        return new HikariDataSource(config);
    }

    private record ValidationResult(SqlStatement statement, boolean success, String errorMessage, String databaseLabel) {
        static ValidationResult success(SqlStatement stmt, String dbLabel) {
            return new ValidationResult(stmt, true, null, dbLabel);
        }

        static ValidationResult failure(SqlStatement stmt, String dbLabel, String error) {
            return new ValidationResult(stmt, false, error, dbLabel);
        }
    }

    private record ValidationSummary(String label, int total, int failures) {
        String toJson() {
            return "{\"label\":\"" + escape(label) + "\",\"total\":" + total + ",\"failures\":" + failures + "}";
        }
    }
}
