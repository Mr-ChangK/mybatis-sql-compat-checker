package io.github.chedwick.sqlcompat;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a mapped SQL statement extracted from a MyBatis mapper XML.
 */
public final class SqlStatement {
    public enum Kind {
        SELECT, INSERT, UPDATE, DELETE, UNKNOWN;

        public static Kind fromTagName(String tag) {
            if (tag == null) {
                return UNKNOWN;
            }
            return switch (tag.toLowerCase()) {
                case "select" -> SELECT;
                case "insert" -> INSERT;
                case "update" -> UPDATE;
                case "delete" -> DELETE;
                default -> UNKNOWN;
            };
        }
    }

    private final String id;
    private final String namespace;
    private final Kind kind;
    private final Path sourceFile;
    private final String rawSql;
    private final List<ParameterSpec> parameters;

    public SqlStatement(String id, String namespace, Kind kind, Path sourceFile, String rawSql, List<ParameterSpec> parameters) {
        this.id = Objects.requireNonNull(id, "id");
        this.namespace = namespace == null ? "" : namespace;
        this.kind = Objects.requireNonNull(kind, "kind");
        this.sourceFile = Objects.requireNonNull(sourceFile, "sourceFile");
        this.rawSql = Objects.requireNonNull(rawSql, "rawSql");
        this.parameters = List.copyOf(parameters == null ? Collections.emptyList() : parameters);
    }

    public String id() {
        return id;
    }

    public Kind kind() {
        return kind;
    }

    public String namespace() {
        return namespace;
    }

    public Path sourceFile() {
        return sourceFile;
    }

    public String fullId() {
        return namespace.isBlank() ? id : namespace + "." + id;
    }

    public String rawSql() {
        return rawSql;
    }

    public List<ParameterSpec> parameters() {
        return parameters;
    }

    @Override
    public String toString() {
        return fullId() + " (" + kind + ") from " + sourceFile;
    }
}
