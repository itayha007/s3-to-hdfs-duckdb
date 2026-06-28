package org.example.service;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds a DuckDB {@code SELECT} that "explodes" every array in an Avro record schema
 * into one row per element, entirely in SQL.
 *
 * <p>Semantics (matching the kafka-s3-flink2 array-explosion test fixtures):
 * <ul>
 *   <li>Each array field is UNNESTed — one output row per element.</li>
 *   <li>Sibling arrays at the same level produce a Cartesian product.</li>
 *   <li>Arrays nested inside record elements are recursively exploded, and the
 *       record is rebuilt as a struct holding the (now scalar/exploded) values.</li>
 *   <li>Empty or null arrays are ignored — the row is preserved with a NULL value
 *       rather than being dropped (so a single empty array can't collapse the row count).</li>
 *   <li>Fields without any array anywhere in their type pass through untouched.</li>
 * </ul>
 *
 * <p>Structure is read straight off the Avro {@link Schema} (element types, nested records,
 * null-unions), so no DuckDB type-string parsing is needed. The DuckDB element type required
 * for the empty-array guard is produced by {@link AvroToDuckDbConverter#toType(Schema)}.
 *
 * <p>Cartesian explosion is expressed with implicit-lateral comma joins:
 * <pre>
 *   FROM _staging,
 *        UNNEST(...orders...)            AS expl_1(v),
 *        UNNEST(expl_1.v.products)       AS expl_2(v),
 *        UNNEST(expl_1.v.discounts)      AS expl_3(v)
 * </pre>
 * Each {@code UNNEST} may reference columns produced by earlier ones (DuckDB treats
 * UNNEST in a comma list as lateral), which gives nested per-parent explosion.
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
        List<String> selectItems = new ArrayList<>();
        for (Schema.Field field : recordSchema.getFields()) {
            String colExpr = sourceRelation + "." + quoteId(field.name());
            String value = genValue(colExpr, unwrapNull(field.schema()));
            selectItems.add(value + " AS " + quoteId(field.name()));
        }
        return "SELECT " + String.join(", ", selectItems) + " FROM " + sourceRelation + joins;
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
        String guarded = "CASE WHEN " + expr + " IS NULL OR len(" + expr + ") = 0"
                + " THEN [CAST(NULL AS " + elementSql + ")] ELSE " + expr + " END";
        String alias = "expl_" + (++aliasSeq);
        joins.append(", UNNEST(").append(guarded).append(") AS ").append(alias).append("(v)");
        // Recurse into the element so nested arrays inside record elements are also exploded.
        return genValue(alias + ".v", unwrapNull(element));
    }

    private String genRecord(String expr, Schema record) {
        if (!containsArray(record)) {
            return expr; // no array anywhere inside — pass the struct through untouched
        }
        StringBuilder lit = new StringBuilder("{");
        List<Schema.Field> fields = record.getFields();
        for (int i = 0; i < fields.size(); i++) {
            Schema.Field f = fields.get(i);
            if (i > 0) lit.append(", ");
            String fieldExpr = "struct_extract(" + expr + ", " + quoteStr(f.name()) + ")";
            lit.append(quoteStr(f.name())).append(": ").append(genValue(fieldExpr, unwrapNull(f.schema())));
        }
        return lit.append("}").toString();
    }

    private static boolean containsArray(Schema schema) {
        switch (schema.getType()) {
            case ARRAY:
                return true;
            case RECORD:
                for (Schema.Field f : schema.getFields()) {
                    if (containsArray(unwrapNull(f.schema()))) {
                        return true;
                    }
                }
                return false;
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
