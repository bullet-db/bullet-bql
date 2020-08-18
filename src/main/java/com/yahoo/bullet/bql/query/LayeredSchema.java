/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
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
        if (schema == null) {
            // If the schema is null, ignore the subschema and just return Type.UNKNOWN
            return Type.UNKNOWN;
        }
        Type type = schema.getType(field);
        if (type != Type.NULL) {
            return type;
        }
        return subSchema != null ? subSchema.getType(field) : Type.NULL;
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
