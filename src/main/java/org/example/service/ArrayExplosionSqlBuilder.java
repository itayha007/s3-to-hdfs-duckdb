package org.example.service;

import org.apache.avro.Schema;
import java.util.stream.Collectors;

/**
 * Builds a DuckDB {@code SELECT} that "explodes" every array in an Avro record schema
 * into one row per element, entirely in SQL.
 */
final class ArrayExplosionSqlBuilder {

    private final StringBuilder joins = new StringBuilder();
    private int aliasSeq = 0;

    private ArrayExplosionSqlBuilder() {
    }

    /** Builds {@code SELECT <exploded columns> FROM <sourceRelation> <lateral unnest joins>}. */
    static String buildExplodedSelect(String sourceRelation, Schema recordSchema) {
        return new ArrayExplosionSqlBuilder().build(sourceRelation, recordSchema);
    }

    private String build(String sourceRelation, Schema recordSchema) {
        String selectItems = recordSchema.getFields().stream()
                .map(field -> {
                    String colExpr = sourceRelation + "." + quoteId(field.name());
                    String value = genValue(colExpr, unwrapNull(field.schema()));
                    return value + " AS " + quoteId(field.name());
                })
                .collect(Collectors.joining(", "));

        return "SELECT " + selectItems + " FROM " + sourceRelation + joins;
    }

    /**
     * Returns a SQL expression for the exploded value of {@code expr} (of Avro type {@code type},
     * already null-union-unwrapped), appending any UNNEST joins it needs to {@link #joins}.
     */
    private String genValue(String expr, Schema type) {
        switch (type.getType()) {
            case ARRAY:
                return genArray(expr, type);
            case RECORD:
                return genRecord(expr, type);
            default:
                return expr; // scalar / map / enum — nothing to explode
        }
    }

    private String genArray(String expr, Schema arraySchema) {
        Schema element = arraySchema.getElementType();
        String elementSql = AvroToDuckDbConverter.toType(element);

        // Preserve the row for empty/null arrays by unnesting a single NULL element instead.
        String guarded = String.format("CASE WHEN %s IS NULL OR len(%s) = 0 THEN [CAST(NULL AS %s)] ELSE %s END",
                expr, expr, elementSql, expr);

        String alias = "expl_" + (++aliasSeq);
        joins.append(", UNNEST(").append(guarded).append(") AS ").append(alias).append("(v)");

        // Recurse into the element so nested arrays inside record elements are also exploded.
        return genValue(alias + ".v", unwrapNull(element));
    }

    private String genRecord(String expr, Schema record) {
        if (!containsArray(record)) {
            return expr; // no array anywhere inside — pass the struct through untouched
        }

        return record.getFields().stream()
                .map(f -> {
                    String fieldExpr = "struct_extract(" + expr + ", " + quoteStr(f.name()) + ")";
                    return quoteStr(f.name()) + ": " + genValue(fieldExpr, unwrapNull(f.schema()));
                })
                .collect(Collectors.joining(", ", "{", "}"));
    }

    private static boolean containsArray(Schema schema) {
        switch (schema.getType()) {
            case ARRAY:
                return true;
            case RECORD:
                return schema.getFields().stream()
                        .anyMatch(f -> containsArray(unwrapNull(f.schema())));
            default:
                return false;
        }
    }

    /** Unwraps a {@code ["null", T]} union to {@code T}; returns the schema unchanged otherwise. */
    private static Schema unwrapNull(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) {
            return schema;
        }
        return schema.getTypes().stream()
                .filter(s -> s.getType() != Schema.Type.NULL)
                .findFirst()
                .orElse(schema);
    }

    private static String quoteId(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    private static String quoteStr(String s) {
        return "'" + s.replace("'", "''") + "'";
    }
}