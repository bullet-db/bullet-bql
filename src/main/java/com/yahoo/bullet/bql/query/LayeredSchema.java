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

@AllArgsConstructor
public class LayeredSchema {
    private Schema schema;
    private Map<String, String> aliases;
    private LayeredSchema subSchema;
    private int depth;
    private boolean locked;
    private static final int TOP_LAYER = 0;

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

    /**
     * Constructor.
     *
     * @param schema The {@link Schema} to use.
     */
    public LayeredSchema(Schema schema) {
        this.schema = schema;
        this.aliases = Collections.emptyMap();
        this.depth = TOP_LAYER;
    }

    /**
     * Adds a new layer to the top of this, pushing every layer one deeper. Note that if this layered schema had layers
     * on top, their depths will not be adjusted. It is recommended to add layers from the top.
     *
     * @param newSchema The new {@link Schema} to add to the top layer.
     * @param newAliases The new {@link Map} of aliases to add to the top layer.
     */
    public void addLayer(Schema newSchema, Map<String, String> newAliases) {
        subSchema = new LayeredSchema(schema, aliases, subSchema, depth, locked);
        schema = newSchema;
        aliases = newAliases;
        locked = false;
        subSchema.increaseDepth();
    }

    /**
     * Locks this {@link LayeredSchema}, preventing access to all layers below.
     */
    public void lock() {
        locked = true;
    }

    /**
     * Unlocks this {@link LayeredSchema}, allowing access to layers below.
     */
    public void unlock() {
        locked = false;
    }

    /**
     * Gets the current depth of this {@link LayeredSchema}. Depth is defined starting at 0 for the top layer and
     * increases as you go deeper.
     *
     * @return The depth of this layer.
     */
    public int depth() {
        return depth;
    }

    /**
     * Searches for the given field from this layer. The minimum depth parameter can be provided to ensure that
     * the field, if found, is at that depth or greater. The depth is the depth of this layered schema as defined by
     * {@link #depth()}. This can be used to skip layers for the search.
     *
     * @param field The field to search for.
     * @param minimumDepth The minimum (whole number) for the depth to find the field from.
     * @return A {@link FieldLocation} for the field. It is non-null. If the schema does not exist, the type will be
     *         {@link Type#UNKNOWN}. If field is not found, the type will be be {@link Type#NULL}.
     */
    public FieldLocation findField(String field, int minimumDepth) {
        if (schema == null) {
            // If the schema is null, ignore the subschema and just return Type.UNKNOWN
            return FieldLocation.from(null, Type.UNKNOWN, depth);
        }
        if (depth >= minimumDepth) {
            Type type = schema.getType(field);
            if (type != Type.NULL) {
                return FieldLocation.from(new Schema.PlainField(field, type), type, depth);
            }
            String alias = aliases.get(field);
            if (alias != null) {
                type = schema.getType(alias);
                return FieldLocation.from(new Schema.PlainField(alias, type), type, depth);
            }
        }
        return canGoDeeper() ? subSchema.findField(field, minimumDepth) : FieldLocation.from(null, Type.NULL, depth);
    }

    /**
     * Searches for the given field in this layer and below.
     *
     * @param field The field to search for.
     * @return A {@link FieldLocation} for the field. It is non-null. If the schema does not exist, the type will be
     *         {@link Type#UNKNOWN}. If field is not found, the type will be be {@link Type#NULL}.
     */
    public FieldLocation findField(String field) {
        // No depth requirement
        return findField(field, depth);
    }

    /**
     * Searches for the given field in this layer and below.
     *
     * @param field The field to search for.
     * @return The {@link Schema.Field} or null if not found.
     */
    public Schema.Field getField(String field) {
        return findField(field).getField();
    }

    /**
     * Searches for the type of the given field in this layer and below.
     *
     * @param field The field to search for.
     * @return The {@link Type} or {@link Type#NULL} if not found, or if the schema is absent, {@link Type#UNKNOWN}.
     */
    public Type getType(String field) {
        return findField(field).getType();
    }

    /**
     * Checks to see if the given field exists in this layer or below.
     *
     * @param field The field to search for.
     * @return A boolean denoting if the field exists or not.
     */
    public boolean hasField(String field) {
        return findField(field).getField() != null;
    }

    /**
     * Adds a new field to the {@link Schema} at this layer.
     *
     * @param field The name of the field to add.
     * @param type The {@link Type} of the field to add.
     */
    public void addField(String field, Type type) {
        if (schema != null) {
            schema.addField(field, type);
        }
    }

    /**
     * Retrieves the names of all the fields in this and accessible layers below.
     *
     * @return The {@link Set} of field names after flattening.
     */
    public Set<String> getFieldNames() {
        Set<String> fields = new HashSet<>();
        if (canGoDeeper()) {
            fields.addAll(subSchema.getFieldNames());
        }
        if (schema != null) {
            schema.getFields().stream().map(Schema.Field::getName).forEach(fields::add);
        }
        return fields;
    }

    /**
     * Retrieves field names that have aliases but do not exist in the schema at each accessible layer.
     *
     * @return The {@link Set} of extraneous aliases.
     */
    public Set<String> getExtraneousAliases() {
        Set<String> fields = new HashSet<>();
        if (canGoDeeper()) {
            fields.addAll(subSchema.getExtraneousAliases());
        }
        if (schema != null) {
            aliases.keySet().stream().filter(field -> !schema.hasField(field)).forEach(fields::add);
        }
        return fields;
    }

    private void increaseDepth() {
        depth++;
        if (subSchema != null) {
            subSchema.increaseDepth();
        }
    }

    private boolean canGoDeeper() {
        return subSchema != null && !locked;
    }
}
