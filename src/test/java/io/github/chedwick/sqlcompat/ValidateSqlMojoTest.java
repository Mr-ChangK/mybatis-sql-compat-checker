package io.github.chedwick.sqlcompat;

import org.apache.maven.plugin.logging.SystemStreamLog;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.List;
import java.util.Objects;
import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ValidateSqlMojoTest {

    @Test
    public void executesStatementsWithoutReportFile() throws Exception {
        Path mapperFile = Path.of(Objects.requireNonNull(
                ValidateSqlMojoTest.class.getClassLoader().getResource("mappers/SampleMapper.xml"))
                .toURI());
        Path mapperDir = mapperFile.getParent();

        ValidateSqlMojo mojo = Mockito.spy(new ValidateSqlMojo());
        mojo.setLog(new SystemStreamLog());
        setField(mojo, "jdbcUrl", "jdbc:test:target");
        setField(mojo, "username", "target_user");
        setField(mojo, "password", "target_pass");
        setField(mojo, "originJdbcUrl", "jdbc:test:origin");
        setField(mojo, "originUsername", "origin_user");
        setField(mojo, "originPassword", "origin_pass");
        setField(mojo, "mapperDirectories", List.of(mapperDir.toString()));
        setField(mojo, "includes", List.of("**/*Mapper.xml"));
        setField(mojo, "excludes", List.of());
        setField(mojo, "reportPath", null); // avoid filesystem writes in test
        setField(mojo, "executeStatements", false);
        setField(mojo, "threadCount", 2);

        PreparedStatement originPs = mock(PreparedStatement.class);
        PreparedStatement targetPs = mock(PreparedStatement.class);
        Connection originConn = mock(Connection.class);
        Connection targetConn = mock(Connection.class);
        Savepoint originSp = mock(Savepoint.class);
        Savepoint targetSp = mock(Savepoint.class);
        when(originConn.prepareStatement(anyString())).thenReturn(originPs);
        when(targetConn.prepareStatement(anyString())).thenReturn(targetPs);
        when(originConn.setSavepoint(anyString())).thenReturn(originSp);
        when(targetConn.setSavepoint(anyString())).thenReturn(targetSp);
        DataSource originDs = mock(DataSource.class);
        DataSource targetDs = mock(DataSource.class);
        when(originDs.getConnection()).thenReturn(originConn);
        when(targetDs.getConnection()).thenReturn(targetConn);
        Mockito.doReturn(originDs).when(mojo).createDataSource(eq("jdbc:test:origin"), eq("origin_user"), eq("origin_pass"));
        Mockito.doReturn(targetDs).when(mojo).createDataSource(eq("jdbc:test:target"), eq("target_user"), eq("target_pass"));

        assertDoesNotThrow(mojo::execute);

        verify(originPs, atLeastOnce()).setQueryTimeout(eq(8));
        verify(targetPs, atLeastOnce()).setQueryTimeout(eq(8));
        verify(originPs, atLeastOnce()).setObject(eq(1), Mockito.any());
        verify(targetPs, atLeastOnce()).setObject(eq(1), Mockito.any());
        verify(originConn, atLeastOnce()).rollback();
        verify(targetConn, atLeastOnce()).rollback();
    }

    @Test
    public void skipsTargetWhenOriginFails() throws Exception {
        Path mapperFile = Path.of(Objects.requireNonNull(
                ValidateSqlMojoTest.class.getClassLoader().getResource("mappers/SampleMapper.xml"))
                .toURI());
        Path mapperDir = mapperFile.getParent();

        ValidateSqlMojo mojo = Mockito.spy(new ValidateSqlMojo());
        mojo.setLog(new SystemStreamLog());
        setField(mojo, "jdbcUrl", "jdbc:test:target");
        setField(mojo, "username", "target_user");
        setField(mojo, "password", "target_pass");
        setField(mojo, "originJdbcUrl", "jdbc:test:origin");
        setField(mojo, "originUsername", "origin_user");
        setField(mojo, "originPassword", "origin_pass");
        setField(mojo, "mapperDirectories", List.of(mapperDir.toString()));
        setField(mojo, "includes", List.of("**/*Mapper.xml"));
        setField(mojo, "excludes", List.of());
        setField(mojo, "reportPath", null); // avoid filesystem writes in test
        setField(mojo, "executeStatements", false);
        setField(mojo, "threadCount", 2);

        PreparedStatement originPs = mock(PreparedStatement.class);
        Connection originConn = mock(Connection.class);
        Savepoint originSp = mock(Savepoint.class);
        when(originConn.prepareStatement(anyString())).thenReturn(originPs);
        when(originConn.setSavepoint(anyString())).thenReturn(originSp);
        Mockito.doThrow(new SQLException("boom")).when(originPs).execute();
        DataSource originDs = mock(DataSource.class);
        DataSource targetDs = mock(DataSource.class);
        when(originDs.getConnection()).thenReturn(originConn);
        Mockito.doThrow(new AssertionError("Target should not be called")).when(targetDs).getConnection();
        Mockito.doReturn(originDs).when(mojo).createDataSource(eq("jdbc:test:origin"), eq("origin_user"), eq("origin_pass"));
        Mockito.doReturn(targetDs).when(mojo).createDataSource(eq("jdbc:test:target"), eq("target_user"), eq("target_pass"));

        assertDoesNotThrow(mojo::execute);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
