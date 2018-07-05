/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.tree.Expression;
import com.yahoo.bullet.bql.tree.Identifier;
import com.yahoo.bullet.bql.tree.Node;
import com.yahoo.bullet.parsing.Projection;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class ProjectionExtractor {
    private Set<Expression> selectFields;
    private Map<Node, Identifier> aliases;

    /**
     * Constructor that requires a set of selectFields and map of aliases.
     *
     * @param selectFields The non-null Set of selected fields.
     * @param aliases      The non-null Map of aliases.
     * @throws NullPointerException when any of selectFields and aliases is null.
     */
    public ProjectionExtractor(Set<Expression> selectFields, Map<Node, Identifier> aliases) throws NullPointerException {
        requireNonNull(selectFields);
        requireNonNull(aliases);

        this.selectFields = selectFields;
        this.aliases = aliases;
    }

    /**
     * Extract a Projection.
     *
     * @return A Projection based on selectFields and aliases.
     */
    public Projection extractProjection() {
        Projection projection = new Projection();
        Map<String, String> fields = createFields();
        projection.setFields(fields);
        return projection;
    }

    private Map<String, String> createFields() {
        Map<String, String> fields = new HashMap<>();
        for (Expression column : selectFields) {
            fields.put(column.toFormatlessString(), getAlias(column));
        }
        return fields;
    }

    private String getAlias(Expression column) {
        if (aliases.containsKey(column)) {
            return aliases.get(column).toFormatlessString();
        } else {
            return column.toFormatlessString();
        }
    }
}
