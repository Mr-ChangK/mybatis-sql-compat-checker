package io.github.chedwick.sqlcompat;

import java.util.Locale;
import java.util.Objects;

/**
 * Parameter placeholder parsed from a MyBatis expression like #{name,jdbcType=VARCHAR}.
 */
public final class ParameterSpec {
    private final String name;
    private final String jdbcType;

    public ParameterSpec(String name, String jdbcType) {
        this.name = Objects.requireNonNull(name, "name");
        this.jdbcType = jdbcType == null ? null : jdbcType.toUpperCase(Locale.ROOT);
    }

    public String name() {
        return name;
    }

    public String jdbcType() {
        return jdbcType;
    }

    public static ParameterSpec fromToken(String token) {
        if (token == null || token.isBlank()) {
            return new ParameterSpec("param", null);
        }
        String trimmed = token.trim();
        String name = trimmed;
        String jdbcType = null;
        int comma = trimmed.indexOf(',');
        if (comma >= 0) {
            name = trimmed.substring(0, comma).trim();
            String[] parts = trimmed.substring(comma + 1).split(",");
            for (String part : parts) {
                String[] kv = part.split("=");
                if (kv.length == 2 && "jdbcType".equalsIgnoreCase(kv[0].trim())) {
                    jdbcType = kv[1].trim();
                }
            }
        }
        return new ParameterSpec(name.isEmpty() ? "param" : name, jdbcType);
    }

    @Override
    public String toString() {
        return jdbcType == null ? name : name + ":" + jdbcType;
    }
}
