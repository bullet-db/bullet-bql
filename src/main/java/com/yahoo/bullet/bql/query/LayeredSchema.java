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
import java.util.stream.Collectors;

@Getter
@AllArgsConstructor
public class LayeredSchema {
    private Schema schema;
    private Map<String, String> aliases;
    private LayeredSchema subSchema;
    private boolean locked;

    public LayeredSchema(Schema schema) {
        this.schema = schema;
        this.aliases = Collections.emptyMap();
    }

    public void addLayer(Schema newSchema, Map<String, String> newAliases) {
        subSchema = new LayeredSchema(schema, aliases, subSchema, locked);
        schema = newSchema;
        aliases = newAliases;
        locked = false;
    }

    public void lock() {
        locked = true;
    }

    public Schema.Field getField(String field) {
        if (schema == null) {
            return null;
        }
        Type type = schema.getType(field);
        if (type != Type.NULL) {
            return new Schema.PlainField(field, type);
        }
        String alias = aliases.get(field);
        if (alias != null) {
            return new Schema.PlainField(alias, schema.getType(alias));
        }
        return subSchema != null && !subSchema.locked ? subSchema.getField(field) : null;
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
        String alias = aliases.get(field);
        if (alias != null) {
            return schema.getType(alias);
        }
        return subSchema != null && !subSchema.locked ? subSchema.getType(field) : Type.NULL;
    }

    public boolean hasField(String field) {
        if (schema == null) {
            return false;
        }
        if (schema.hasField(field)) {
            return true;
        }
        if (aliases.containsKey(field)) {
            return true;
        }
        return subSchema != null && !subSchema.locked && subSchema.hasField(field);
    }

    public Set<String> getFields() {
        Set<String> fields = new HashSet<>();
        if (subSchema != null && !subSchema.locked) {
            fields.addAll(subSchema.getFields());
        }
        schema.getFields().stream()
                          .map(Schema.Field::getName)
                          .forEach(fields::add);
        return fields;
    }
}
