/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/With.java
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class With extends Node {
    private final boolean recursive;
    private final List<WithQuery> queries;

    /**
     * Constructor that requires a boolean recursive and a List of {@link WithQuery}.
     *
     * @param recursive A boolean.
     * @param queries   A List of {@link WithQuery}.
     */
    public With(boolean recursive, List<WithQuery> queries) {
        this(Optional.empty(), recursive, queries);
    }

    /**
     * Constructor that requires a {@link NodeLocation}, a boolean recursive and a List of {@link WithQuery}.
     *
     * @param location  A {@link NodeLocation}.
     * @param recursive A boolean.
     * @param queries   A List of {@link WithQuery}.
     */
    public With(NodeLocation location, boolean recursive, List<WithQuery> queries) {
        this(Optional.of(location), recursive, queries);
    }

    private With(Optional<NodeLocation> location, boolean recursive, List<WithQuery> queries) {
        super(location);
        requireNonNull(queries, "queries is null");
        checkArgument(!queries.isEmpty(), "queries is empty");

        this.recursive = recursive;
        this.queries = ImmutableList.copyOf(queries);
    }

    /**
     * Get the {@link #recursive} of this With.
     *
     * @return A boolean.
     */
    public boolean isRecursive() {
        return recursive;
    }

    /**
     * Get the {@link #queries} of this With.
     *
     * @return A List of {@link WithQuery}.
     */
    public List<WithQuery> getQueries() {
        return queries;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitWith(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return queries;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        With o = (With) obj;
        return Objects.equals(recursive, o.recursive) &&
                Objects.equals(queries, o.queries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(recursive, queries);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("recursive", recursive)
                .add("queries", queries)
                .toString();
    }
}
