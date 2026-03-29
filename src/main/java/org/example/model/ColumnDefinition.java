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
    /** False when the Avro field is non-union and has no default — must not be null in the JSON. */
    private boolean nullable;
}
