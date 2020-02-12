package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.typesystem.Schema;
import com.yahoo.bullet.typesystem.Type;

public class LayeredSchema {
    private Schema schema;
    private LayeredSchema subSchema;

    public LayeredSchema(Schema schema) {
        this.schema = schema;
    }

    public LayeredSchema(Schema schema, LayeredSchema subSchema) {
        this.schema = schema;
        this.subSchema = subSchema;
    }

    public Type getType(String field) {
        Type type = schema.getType(field);
        if (type != Type.NULL) {
            return type;
        }
        return subSchema != null ? subSchema.getType(field) : Type.NULL;
    }

    public boolean hasField(String field) {
        return schema.hasField(field) || (subSchema != null && subSchema.hasField(field));
    }

    // Add new schema on top
    public void addLayer(Schema newSchema) {
        subSchema = new LayeredSchema(schema, subSchema);
        schema = newSchema;
    }

    public void replaceSchema(Schema newSchema) {
        schema = newSchema;
        subSchema = null;
    }
}
