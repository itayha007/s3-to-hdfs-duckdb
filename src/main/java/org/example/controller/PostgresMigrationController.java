package org.example.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.example.service.PostgresMigrationService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;

@Log4j2
@RestController
@RequestMapping("/api/migrate")
@RequiredArgsConstructor
public class PostgresMigrationController {

    private final PostgresMigrationService migrationService;

    /**
     * Triggers a table migration from the backup PostgreSQL server to the main server.
     *
     * <pre>
     * POST /api/migrate?table=orders
     * </pre>
     */
    @PostMapping
    public ResponseEntity<String> migrate(@RequestParam String table) {
        try {
            migrationService.migrate(table);
            return ResponseEntity.ok("Migration complete: table '" + table + "' transferred successfully");
        } catch (IllegalArgumentException e) {
            log.warn("Bad migration request for table '{}': {}", table, e.getMessage());
            return ResponseEntity.badRequest().body(e.getMessage());
        } catch (SQLException e) {
            log.error("Migration failed for table '{}'", table, e);
            return ResponseEntity.internalServerError().body("Migration failed: " + e.getMessage());
        }
    }
}
