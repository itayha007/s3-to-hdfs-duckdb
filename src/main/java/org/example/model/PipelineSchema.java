package org.example.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.avro.Schema;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PipelineSchema {
    private String pipelineName;
    private List<ColumnDefinition> columns;
    /** Source Avro schema, used by the array-explosion step to walk nested structure. */
    private Schema avroSchema;
}
