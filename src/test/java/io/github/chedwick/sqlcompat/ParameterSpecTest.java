package io.github.chedwick.sqlcompat;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ParameterSpecTest {

    @Test
    public void fromTokenParsesNameAndJdbcType() {
        ParameterSpec spec = ParameterSpec.fromToken("bookId,jdbcType=VARCHAR");

        assertEquals("bookId", spec.name());
        assertEquals("VARCHAR", spec.jdbcType());
    }

    @Test
    public void fromTokenDefaultsWhenBlank() {
        ParameterSpec spec = ParameterSpec.fromToken("  ");

        assertEquals("param", spec.name());
        assertNull(spec.jdbcType());
    }
}
