package org.example.service;

import org.example.model.ColumnDefinition;
import org.example.model.PipelineSchema;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds a DuckDB {@code SELECT} that "explodes" every array in a {@link PipelineSchema}
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
 * <p>The explosion is driven purely off the DuckDB type strings in
 * {@link ColumnDefinition#getDuckDbType()} (e.g. {@code STRUCT(id INTEGER, children VARCHAR[])}),
 * so no Avro schema is needed at explosion time.
 *
 * <p>Cartesian explosion is expressed with implicit-lateral comma joins:
 * <pre>
 *   FROM _staging,
 *        UNNEST(...orders...)   AS expl_1(v),
 *        UNNEST(expl_1.v.products)  AS expl_2(v),
 *        UNNEST(expl_1.v.discounts) AS expl_3(v)
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
    static String buildExplodedSelect(String sourceRelation, PipelineSchema schema) {
        return new ArrayExplosionSqlBuilder().build(sourceRelation, schema);
    }

    private String build(String sourceRelation, PipelineSchema schema) {
        List<String> selectItems = new ArrayList<>();
        for (ColumnDefinition col : schema.getColumns()) {
            DType type = TypeParser.parse(col.getDuckDbType());
            String colExpr = sourceRelation + "." + quoteId(col.getName());
            selectItems.add(genValue(colExpr, type) + " AS " + quoteId(col.getName()));
        }
        return "SELECT " + String.join(", ", selectItems)
                + " FROM " + sourceRelation + joins;
    }

    /**
     * Returns a SQL expression for the exploded value of {@code expr} (of type {@code type}),
     * appending any UNNEST joins it needs to {@link #joins}.
     */
    private String genValue(String expr, DType type) {
        if (!type.containsArray()) {
            return expr; // nothing to explode — pass through untouched
        }
        if (type instanceof StructType) {
            StructType st = (StructType) type;
            StringBuilder lit = new StringBuilder("{");
            for (int i = 0; i < st.fields.size(); i++) {
                StructField f = st.fields.get(i);
                if (i > 0) lit.append(", ");
                String fieldExpr = "struct_extract(" + expr + ", " + quoteStr(f.name) + ")";
                lit.append(quoteStr(f.name)).append(": ").append(genValue(fieldExpr, f.type));
            }
            return lit.append("}").toString();
        }
        // ArrayType: UNNEST one element per row, preserving the row for empty/null arrays.
        ArrayType at = (ArrayType) type;
        String guarded = "CASE WHEN " + expr + " IS NULL OR len(" + expr + ") = 0"
                + " THEN [CAST(NULL AS " + at.element.sql + ")] ELSE " + expr + " END";
        String alias = "expl_" + (++aliasSeq);
        joins.append(", UNNEST(").append(guarded).append(") AS ").append(alias).append("(v)");
        // Recurse into the element so nested arrays inside record elements are also exploded.
        return genValue(alias + ".v", at.element);
    }

    private static String quoteId(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    private static String quoteStr(String s) {
        return "'" + s.replace("'", "''") + "'";
    }

    // =========================================================================
    // DuckDB type tree + parser
    // =========================================================================

    abstract static class DType {
        /** Canonical DuckDB type text, e.g. {@code VARCHAR}, {@code VARCHAR[]}, {@code STRUCT(a INTEGER)}. */
        final String sql;

        DType(String sql) {
            this.sql = sql;
        }

        abstract boolean containsArray();
    }

    static final class ScalarType extends DType {
        ScalarType(String sql) {
            super(sql);
        }

        @Override
        boolean containsArray() {
            return false;
        }
    }

    static final class ArrayType extends DType {
        final DType element;

        ArrayType(DType element, String sql) {
            super(sql);
            this.element = element;
        }

        @Override
        boolean containsArray() {
            return true;
        }
    }

    static final class StructType extends DType {
        final List<StructField> fields;

        StructType(List<StructField> fields, String sql) {
            super(sql);
            this.fields = fields;
        }

        @Override
        boolean containsArray() {
            for (StructField f : fields) {
                if (f.type.containsArray()) {
                    return true;
                }
            }
            return false;
        }
    }

    static final class StructField {
        final String name;
        final DType type;

        StructField(String name, DType type) {
            this.name = name;
            this.type = type;
        }
    }

    /** Minimal recursive-descent parser for the DuckDB type strings emitted by AvroToDuckDbConverter. */
    static final class TypeParser {
        private final String s;
        private int pos;

        private TypeParser(String s) {
            this.s = s;
        }

        static DType parse(String typeText) {
            return new TypeParser(typeText.trim()).parseType();
        }

        private DType parseType() {
            DType base = parseBase();
            skipWs();
            while (peek() == '[') {
                expect('[');
                skipWs();
                expect(']');
                base = new ArrayType(base, base.sql + "[]");
                skipWs();
            }
            return base;
        }

        private DType parseBase() {
            skipWs();
            String word = readWord();
            if (word.equalsIgnoreCase("STRUCT")) {
                return parseStruct(word);
            }
            // MAP / DECIMAL / etc. — capture any parameter list verbatim and treat as a scalar
            // (we never explode inside maps).
            skipWs();
            if (peek() == '(') {
                return new ScalarType(word + readBalancedParens());
            }
            return new ScalarType(word);
        }

        private DType parseStruct(String keyword) {
            skipWs();
            expect('(');
            List<StructField> fields = new ArrayList<>();
            skipWs();
            while (peek() != ')') {
                String name = readWord();
                skipWs();
                DType fieldType = parseType();
                fields.add(new StructField(name, fieldType));
                skipWs();
                if (peek() == ',') {
                    expect(',');
                    skipWs();
                }
            }
            expect(')');

            StringBuilder sql = new StringBuilder(keyword).append("(");
            for (int i = 0; i < fields.size(); i++) {
                if (i > 0) sql.append(", ");
                sql.append(fields.get(i).name).append(" ").append(fields.get(i).type.sql);
            }
            return new StructType(fields, sql.append(")").toString());
        }

        private String readBalancedParens() {
            skipWs();
            StringBuilder sb = new StringBuilder();
            expect('(');
            sb.append('(');
            int depth = 1;
            while (depth > 0) {
                char c = s.charAt(pos++);
                sb.append(c);
                if (c == '(') depth++;
                else if (c == ')') depth--;
            }
            return sb.toString();
        }

        private String readWord() {
            skipWs();
            int start = pos;
            while (pos < s.length()) {
                char c = s.charAt(pos);
                if (Character.isLetterOrDigit(c) || c == '_') {
                    pos++;
                } else {
                    break;
                }
            }
            return s.substring(start, pos);
        }

        private char peek() {
            return pos < s.length() ? s.charAt(pos) : '\0';
        }

        private void expect(char c) {
            skipWs();
            if (peek() != c) {
                throw new IllegalStateException(
                        "Malformed DuckDB type '" + s + "': expected '" + c + "' at position " + pos);
            }
            pos++;
        }

        private void skipWs() {
            while (pos < s.length() && Character.isWhitespace(s.charAt(pos))) {
                pos++;
            }
        }
    }
}
