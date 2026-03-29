package org.example.service;

import org.apache.avro.Schema;
import org.example.model.ColumnDefinition;
import org.example.model.PipelineSchema;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts an Avro Schema to a PipelineSchema with DuckDB column type strings.
 * This is the same type mapping Spark uses internally when writing Parquet from Avro —
 * DuckDB doesn't natively consume Avro schemas so we bridge them here.
 */
class AvroToDuckDbConverter {

    static PipelineSchema convert(String pipelineName, Schema avroSchema) {
        List<ColumnDefinition> cols = new ArrayList<>();
        for (Schema.Field field : avroSchema.getFields()) {
            cols.add(new ColumnDefinition(field.name(), toType(field.schema()), isNullable(field)));
        }
        return new PipelineSchema(pipelineName, cols);
    }

    private static boolean isNullable(Schema.Field field) {
        Schema schema = field.schema();
        boolean nullUnion = schema.getType() == Schema.Type.UNION &&
                schema.getTypes().stream().anyMatch(t -> t.getType() == Schema.Type.NULL);
        return nullUnion || field.hasDefaultValue();
    }

    private static String toType(Schema schema) {
        switch (schema.getType()) {
            case STRING:
            case ENUM:      return "VARCHAR";
            case INT:       return "INTEGER";
            case LONG:      return "BIGINT";
            case FLOAT:     return "FLOAT";
            case DOUBLE:    return "DOUBLE";
            case BOOLEAN:   return "BOOLEAN";
            case BYTES:
            case FIXED:     return "BLOB";
            case RECORD:    return toStruct(schema);
            case ARRAY:     return toType(schema.getElementType()) + "[]";
            case MAP:       return "MAP(VARCHAR, " + toType(schema.getValueType()) + ")";
            case UNION:     return toType(nonNull(schema));
            default:        return "VARCHAR";
        }
    }

    private static String toStruct(Schema record) {
        StringBuilder sb = new StringBuilder("STRUCT(");
        List<Schema.Field> fields = record.getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) sb.append(", ");
            Schema.Field f = fields.get(i);
            sb.append(f.name()).append(" ").append(toType(f.schema()));
        }
        return sb.append(")").toString();
    }

    /** Unwraps ["null", "T"] unions — returns T. */
    private static Schema nonNull(Schema union) {
        return union.getTypes().stream()
                .filter(s -> s.getType() != Schema.Type.NULL)
                .findFirst()
                .orElse(Schema.create(Schema.Type.STRING));
    }
}
