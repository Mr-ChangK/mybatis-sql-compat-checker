package io.github.chedwick.sqlcompat;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.PreparedStatement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.verify;

class ParameterGeneratorTest {

    @Test
    public void generateValuesRespectsJdbcTypes() {
        ParameterGenerator generator = new ParameterGenerator();
        List<ParameterSpec> specs = List.of(
                new ParameterSpec("id", "INTEGER"),
                new ParameterSpec("title", "VARCHAR"),
                new ParameterSpec("active", "BOOLEAN"),
                new ParameterSpec("createdAt", "TIMESTAMP")
        );

        List<Object> values = generator.generateValues(specs);

        assertEquals(4, values.size());
        assertInstanceOf(Integer.class, values.get(0));
        assertInstanceOf(String.class, values.get(1));
        assertInstanceOf(Boolean.class, values.get(2));
        assertNotNull(values.get(3));
    }

    @Test
    public void bindSetsObjectsOnPreparedStatement() throws Exception {
        ParameterGenerator generator = new ParameterGenerator();
        List<ParameterSpec> specs = List.of(
                new ParameterSpec("id", "INTEGER"),
                new ParameterSpec("name", "VARCHAR")
        );
        PreparedStatement ps = Mockito.mock(PreparedStatement.class);

        generator.bind(ps, specs);

        verify(ps).setObject(1, 1);
        verify(ps).setObject(2, "sample");
    }
}
