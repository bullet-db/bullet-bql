/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/Query.java
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Query extends Statement {
    private final Optional<With> with;
    private final QueryBody queryBody;
    private final Optional<OrderBy> orderBy;
    private final Optional<String> limit;

    /**
     * Constructor that requires an Optional of {@link With}, a {@link QueryBody}, an Optional of {@link OrderBy} and an Optional of String.
     *
     * @param with      An Optional of {@link With}.
     * @param queryBody A {@link QueryBody}.
     * @param orderBy   An Optional of {@link OrderBy}.
     * @param limit     An Optional of String.
     */
    public Query(
            Optional<With> with,
            QueryBody queryBody,
            Optional<OrderBy> orderBy,
            Optional<String> limit) {
        this(Optional.empty(), with, queryBody, orderBy, limit);
    }

    /**
     * Constructor that requires a {@link NodeLocation}, an Optional of {@link With}, a {@link QueryBody}, an Optional of {@link OrderBy} and an Optional of String.
     *
     * @param location  A {@link NodeLocation}.
     * @param with      An Optional of {@link With}.
     * @param queryBody A {@link QueryBody}.
     * @param orderBy   An Optional of {@link OrderBy}.
     * @param limit     An Optional of String.
     */
    public Query(
            NodeLocation location,
            Optional<With> with,
            QueryBody queryBody,
            Optional<OrderBy> orderBy,
            Optional<String> limit) {
        this(Optional.of(location), with, queryBody, orderBy, limit);
    }

    private Query(
            Optional<NodeLocation> location,
            Optional<With> with,
            QueryBody queryBody,
            Optional<OrderBy> orderBy,
            Optional<String> limit) {
        super(location);
        requireNonNull(with, "with is null");
        requireNonNull(queryBody, "queryBody is null");
        requireNonNull(orderBy, "orderBy is null");
        requireNonNull(limit, "limit is null");

        this.with = with;
        this.queryBody = queryBody;
        this.orderBy = orderBy;
        this.limit = limit;
    }

    /**
     * Get the {@link #with} of this Query.
     *
     * @return An Optional of {@link With}.
     */
    public Optional<With> getWith() {
        return with;
    }

    /**
     * Get the {@link #orderBy} of this Query.
     *
     * @return An Optional of {@link OrderBy}.
     */
    public Optional<OrderBy> getOrderBy() {
        return orderBy;
    }

    /**
     * Get the {@link #limit} of this Query.
     *
     * @return An Optional of String.
     */
    public Optional<String> getLimit() {
        return limit;
    }

    /**
     * Get the {@link QueryBody} of this Query.
     *
     * @return An {@link QueryBody}.
     */
    public QueryBody getQueryBody() {
        return queryBody;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitQuery(this, context);
    }

    @Override
    public List<Node> getChildren() {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        with.ifPresent(nodes::add);
        nodes.add(queryBody);
        orderBy.ifPresent(nodes::add);
        return nodes.build();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("with", with.orElse(null))
                .add("queryBody", queryBody)
                .add("orderBy", orderBy)
                .add("limit", limit.orElse(null))
                .omitNullValues()
                .toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Query o = (Query) obj;
        return Objects.equals(with, o.with) &&
                Objects.equals(queryBody, o.queryBody) &&
                Objects.equals(orderBy, o.orderBy) &&
                Objects.equals(limit, o.limit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(with, queryBody, orderBy, limit);
    }
}
