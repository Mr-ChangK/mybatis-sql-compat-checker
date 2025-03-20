package io.github.chedwick.sqlcompat;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlStatementTest {

    @Test
    public void fullIdPrefixesNamespace() {
        SqlStatement stmt = new SqlStatement(
                "findBook",
                "demo.mapper",
                SqlStatement.Kind.SELECT,
                Path.of("SampleMapper.xml"),
                "SELECT * FROM books",
                List.of(new ParameterSpec("id", "INTEGER"))
        );

        assertEquals("demo.mapper.findBook", stmt.fullId());
        assertTrue(stmt.toString().contains("findBook"));
    }
}
