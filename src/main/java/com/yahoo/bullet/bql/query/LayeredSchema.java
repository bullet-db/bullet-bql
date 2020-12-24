/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.typesystem.Schema;
import com.yahoo.bullet.typesystem.Type;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Getter
@AllArgsConstructor
public class LayeredSchema {
    private Schema schema;
    private Map<String, String> aliases;
    private LayeredSchema subSchema;
    private int depth;
    private boolean locked;

    @Getter
    public static class FieldLocation {
        private Schema.Field field;
        private Type type;
        private int depth;

        private static FieldLocation from(Schema.Field field, Type type, int depth) {
            FieldLocation location = new FieldLocation();
            location.field = field;
            location.type = type;
            location.depth = depth;
            return location;
        }
    }

    public LayeredSchema(Schema schema) {
        this.schema = schema;
        this.aliases = Collections.emptyMap();
        this.depth = 0;
    }

    public void addLayer(Schema newSchema, Map<String, String> newAliases) {
        subSchema = new LayeredSchema(schema, aliases, subSchema, depth + 1, locked);
        schema = newSchema;
        aliases = newAliases;
        locked = false;
        depth = 0;
    }

    public void lock() {
        locked = true;
    }

    public FieldLocation findField(String field) {
        if (schema == null) {
            // If the schema is null, ignore the subschema and just return Type.UNKNOWN
            return FieldLocation.from(null, Type.UNKNOWN, depth);
        }
        Type type = schema.getType(field);
        if (type != Type.NULL) {
            return FieldLocation.from(new Schema.PlainField(field, type), type, depth);
        }
        String alias = aliases.get(field);
        if (alias != null) {
            type = schema.getType(alias);
            return FieldLocation.from(new Schema.PlainField(alias, type), type, depth);
        }
        return subSchema != null && !subSchema.locked ? subSchema.findField(field) : FieldLocation.from(null, Type.NULL, depth);
    }

    public Schema.Field getField(String field) {
        return findField(field).getField();
    }

    public Type getType(String field) {
        return findField(field).getType();
    }

    public boolean hasField(String field) {
        return findField(field).getField() != null;
    }

    public Set<String> getFieldNames() {
        Set<String> fields = new HashSet<>();
        if (subSchema != null && !subSchema.locked) {
            fields.addAll(subSchema.getFieldNames());
        }
        if (schema != null) {
            schema.getFields().stream().map(Schema.Field::getName).forEach(fields::add);
        }
        return fields;
    }
}
