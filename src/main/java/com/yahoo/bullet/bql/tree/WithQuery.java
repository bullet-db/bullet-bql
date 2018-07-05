/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/WithQuery.java
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class WithQuery extends Node {
    private final Identifier name;
    private final Query query;
    private final Optional<List<Identifier>> columnNames;

    /**
     * Constructor that requires an {@link Identifier}, a {@link Query} and an Optional of a List of {@link Identifier} columnNames.
     *
     * @param name        An {@link Identifier}.
     * @param query       A {@link Query}.
     * @param columnNames An Optional of a List of {@link Identifier}.
     */
    public WithQuery(Identifier name, Query query, Optional<List<Identifier>> columnNames) {
        this(Optional.empty(), name, query, columnNames);
    }

    /**
     * Constructor that requires a {@link NodeLocation}, an {@link Identifier}, a {@link Query} and an Optional of a List of {@link Identifier} columnNames.
     *
     * @param location    A {@link NodeLocation}.
     * @param name        An {@link Identifier}.
     * @param query       A {@link Query}.
     * @param columnNames An Optional of a List of {@link Identifier}.
     */
    public WithQuery(NodeLocation location, Identifier name, Query query, Optional<List<Identifier>> columnNames) {
        this(Optional.of(location), name, query, columnNames);
    }

    private WithQuery(Optional<NodeLocation> location, Identifier name, Query query, Optional<List<Identifier>> columnNames) {
        super(location);
        this.name = name;
        this.query = requireNonNull(query, "query is null");
        this.columnNames = requireNonNull(columnNames, "columnNames is null");
    }

    /**
     * Get the {@link #name} of this WithQuery.
     *
     * @return An {@link Identifier}.
     */
    public Identifier getName() {
        return name;
    }

    /**
     * Get the {@link #query} of this WithQuery.
     *
     * @return A {@link Query}.
     */
    public Query getQuery() {
        return query;
    }

    /**
     * Get the {@link #columnNames} of this WithQuery.
     *
     * @return An Optional of a List of {@link Identifier}.
     */
    public Optional<List<Identifier>> getColumnNames() {
        return columnNames;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitWithQuery(this, context);
    }

    @Override
    public List<Node> getChildren() {
        return ImmutableList.of(query);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("name", name)
                .add("query", query)
                .add("columnNames", columnNames)
                .omitNullValues()
                .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, query, columnNames);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        WithQuery o = (WithQuery) obj;
        return Objects.equals(name, o.name) &&
                Objects.equals(query, o.query) &&
                Objects.equals(columnNames, o.columnNames);
    }
}
