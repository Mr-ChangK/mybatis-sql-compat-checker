package io.github.chedwick.sqlcompat;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

/**
 * Produces deterministic sample values for parameters based on jdbcType hints.
 */
public final class ParameterGenerator {

    public List<Object> generateValues(List<ParameterSpec> specs) {
        List<Object> values = new ArrayList<>(specs.size());
        for (ParameterSpec spec : specs) {
            values.add(generateValue(spec));
        }
        return values;
    }

    public void bind(PreparedStatement ps, List<ParameterSpec> specs) throws SQLException {
        List<Object> values = generateValues(specs);
        for (int i = 0; i < values.size(); i++) {
            ps.setObject(i + 1, values.get(i));
        }
    }

    private Object generateValue(ParameterSpec spec) {
        String type = spec.jdbcType();
        if (type == null) {
            return "sample";
        }
        return switch (type.toUpperCase(Locale.ROOT)) {
            case "VARCHAR", "CHAR", "TEXT" -> "sample";
            case "UUID" -> UUID.randomUUID();
            case "INTEGER", "INT", "SMALLINT" -> 1;
            case "BIGINT" -> 1L;
            case "NUMERIC", "DECIMAL" -> BigDecimal.valueOf(1);
            case "BOOLEAN", "BIT" -> Boolean.TRUE;
            case "DATE" -> LocalDate.now();
            case "TIMESTAMP", "TIMESTAMPTZ" -> Timestamp.from(Instant.now());
            case "TIME" -> java.sql.Time.valueOf("00:00:01");
            default -> "sample";
        };
    }
}
