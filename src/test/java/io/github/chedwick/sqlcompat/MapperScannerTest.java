package io.github.chedwick.sqlcompat;

import org.apache.maven.plugin.logging.SystemStreamLog;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MapperScannerTest {

    @Test
    public void scanFindsMappedStatements() throws Exception {
        Path mapperFile = Path.of(Objects.requireNonNull(
                MapperScannerTest.class.getClassLoader().getResource("mappers/SampleMapper.xml"))
                .toURI());
        Path mapperDir = mapperFile.getParent();

        MapperScanner scanner = new MapperScanner(
                new SystemStreamLog(),
                List.of(mapperDir.toString()),
                List.of("**/*Mapper.xml"),
                List.of()
        );

        List<SqlStatement> statements = scanner.scan();

        assertEquals(2, statements.size());
        SqlStatement select = statements.stream()
                .filter(s -> s.id().equals("findBook"))
                .findFirst()
                .orElseThrow();
        assertEquals("findBook", select.id());
        assertEquals(SqlStatement.Kind.SELECT, select.kind());
        assertEquals("demo.mapper", select.namespace());
        assertEquals(1, select.parameters().size());
        assertEquals("id", select.parameters().get(0).name());
        assertNotNull(select.sourceFile());
        assertTrue(Files.exists(select.sourceFile()));
    }

    @Test
    public void scanHandlesIfTags() throws Exception {
        Path mapperFile = Path.of(Objects.requireNonNull(
                MapperScannerTest.class.getClassLoader().getResource("mappers/IfMapper.xml"))
                .toURI());
        Path mapperDir = mapperFile.getParent();

        MapperScanner scanner = new MapperScanner(
                new SystemStreamLog(),
                List.of(mapperDir.toString()),
                List.of("**/*Mapper.xml"),
                List.of()
        );

        List<SqlStatement> statements = scanner.scan();

        assertEquals(1, statements.size());
        SqlStatement stmt = statements.get(0);
        assertEquals("findBooks", stmt.id());
        assertEquals("demo.ifmapper", stmt.namespace());
        assertEquals(SqlStatement.Kind.SELECT, stmt.kind());
        assertEquals(2, stmt.parameters().size());
        assertEquals("id", stmt.parameters().get(0).name());
        assertEquals("INTEGER", stmt.parameters().get(0).jdbcType());
        assertTrue(stmt.rawSql().contains("WHERE id = ?"));
        assertTrue(stmt.rawSql().contains("title LIKE concat('%', ?, '%')"));
    }
}
