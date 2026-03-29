package org.example.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ColumnDefinition {
    private String name;
    /** DuckDB SQL type string, e.g. VARCHAR, BIGINT, TIMESTAMP, STRUCT(x DOUBLE), VARCHAR[] */
    private String duckDbType;
}
