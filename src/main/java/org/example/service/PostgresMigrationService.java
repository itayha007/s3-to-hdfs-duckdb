package org.example.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.example.config.DuckDbConfig;
import org.example.config.PostgresConfig;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Inserts rows from a read-only PostgreSQL replica into a table on the main server using
 * DuckDB's postgres extension. DuckDB attaches both servers and runs a single INSERT INTO SELECT.
 */
@Log4j2
@Service
@RequiredArgsConstructor
public class PostgresMigrationService {

    private final DuckDbConfig duckDbConfig;
    private final PostgresConfig postgresConfig;

    /**
     * Inserts all rows from {@code tableName} on the read-only replica into the
     * already-existing table on the main server.
     *
     * @param tableName unqualified table name (e.g. {@code "orders"})
     */
    public void migrate(String tableName) throws SQLException {
        PostgresConfig.ServerConfig src = postgresConfig.getSource();
        PostgresConfig.ServerConfig tgt = postgresConfig.getTarget();

        log.info("Starting migration: {}.{}.{} -> {}.{}.{}",
                src.getDbname(), src.getSchema(), tableName,
                tgt.getDbname(), tgt.getSchema(), tableName);

        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            installAndLoadPostgresExtension(stmt);
            attachSourceDb(stmt, src);
            attachTargetDb(stmt, tgt);
            copyTable(stmt, src.getSchema(), tgt.getSchema(), tableName);
        }

        log.info("Migration complete: table '{}' transferred successfully", tableName);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void installAndLoadPostgresExtension(Statement stmt) throws SQLException {
        stmt.execute("INSTALL postgres");
        stmt.execute("LOAD postgres");
    }

    private void attachSourceDb(Statement stmt, PostgresConfig.ServerConfig cfg) throws SQLException {
        stmt.execute(String.format(
                "ATTACH '%s' AS source_db (TYPE POSTGRES, READ_ONLY)",
                cfg.toConnectionString()));
    }

    private void attachTargetDb(Statement stmt, PostgresConfig.ServerConfig cfg) throws SQLException {
        stmt.execute(String.format(
                "ATTACH '%s' AS target_db (TYPE POSTGRES)",
                cfg.toConnectionString()));
    }

    private void copyTable(Statement stmt,
                           String sourceSchema, String targetSchema,
                           String tableName) throws SQLException {
        log.info("Loading table '{}' into DuckDB memory", tableName);
        stmt.execute(String.format(
                "CREATE TABLE mem_%s AS SELECT * FROM source_db.%s.%s",
                tableName, sourceSchema, tableName));

        try {
            List<String> dates = queryDistinctDates(stmt, tableName);
            log.info("Found {} distinct date(s) in '__receive_time' for table '{}'", dates.size(), tableName);

            for (String date : dates) {
                String chunkTable = tableName + "_" + date.replace("-", "_");
                log.info("Writing chunk -> '{}'", chunkTable);
                stmt.execute(String.format(
                        "CALL postgres_execute('target_db', 'CREATE TABLE %s.%s (LIKE %s.%s)')",
                        targetSchema, chunkTable, sourceSchema, tableName));
                stmt.execute(String.format(
                        "INSERT INTO target_db.%s.%s SELECT * FROM mem_%s WHERE (\"__receive_time\"::TIMESTAMPTZ AT TIME ZONE 'UTC')::DATE = '%s'",
                        targetSchema, chunkTable, tableName, date));
            }
        } finally {
            stmt.execute("DROP TABLE mem_" + tableName);
        }
    }

    private List<String> queryDistinctDates(Statement stmt, String tableName) throws SQLException {
        List<String> dates = new ArrayList<>();
        try (ResultSet rs = stmt.executeQuery(String.format(
                "SELECT DISTINCT (\"__receive_time\"::TIMESTAMPTZ AT TIME ZONE 'UTC')::DATE AS d FROM mem_%s ORDER BY d", tableName))) {
            while (rs.next()) {
                dates.add(rs.getString(1));
            }
        }
        return dates;
    }

    private long countRows(Statement stmt, String schema, String tableName) throws SQLException {
        try (var rs = stmt.executeQuery(
                String.format("SELECT COUNT(*) FROM source_db.%s.%s", schema, tableName))) {
            return rs.next() ? rs.getLong(1) : 0;
        }
    }

    private Connection connect() throws SQLException {
        Properties props = duckDbConfig.getProperties();
        return DriverManager.getConnection("jdbc:duckdb:", props);
    }
}
