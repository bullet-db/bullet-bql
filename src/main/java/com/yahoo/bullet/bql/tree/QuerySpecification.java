/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/QuerySpecification.java
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class QuerySpecification extends QueryBody {
    private final Select select;
    private final Optional<Relation> from;
    private final Optional<Expression> where;
    private final Optional<GroupBy> groupBy;
    private final Optional<Expression> having;
    private final Optional<OrderBy> orderBy;
    private final Optional<String> limit;
    private final Optional<Windowing> windowing;

    /**
     * Constructor that requires a select, a from, a where, a groupBy, a having, an orderBy, a limit and a windowing.
     *
     * @param select    A {@link Select}.
     * @param from      An Optional of {@link Relation}. Currently, we use {@link Stream}.
     * @param where     An Optional of {@link Expression}.
     * @param groupBy   An Optional of {@link GroupBy}.
     * @param having    An Optional of {@link Expression}.
     * @param orderBy   An Optional of {@link OrderBy}.
     * @param limit     An Optional of String.
     * @param windowing An Optional of {@link Windowing}.
     */
    public QuerySpecification(
            Select select,
            Optional<Relation> from,
            Optional<Expression> where,
            Optional<GroupBy> groupBy,
            Optional<Expression> having,
            Optional<OrderBy> orderBy,
            Optional<String> limit,
            Optional<Windowing> windowing) {
        this(Optional.empty(), select, from, where, groupBy, having, orderBy, limit, windowing);
    }

    /**
     * Constructor that requires a {@link NodeLocation}, a select, a from, a where, a groupBy, a having, an orderBy, a limit and a windowing.
     *
     * @param location  A {@link NodeLocation}.
     * @param select    A {@link Select}.
     * @param from      An Optional of {@link Relation}. Currently, we use {@link Stream}.
     * @param where     An Optional of {@link Expression}.
     * @param groupBy   An Optional of {@link GroupBy}.
     * @param having    An Optional of {@link Expression}.
     * @param orderBy   An Optional of {@link OrderBy}.
     * @param limit     An Optional of String.
     * @param windowing An Optional of {@link Windowing}.
     */
    public QuerySpecification(
            NodeLocation location,
            Select select,
            Optional<Relation> from,
            Optional<Expression> where,
            Optional<GroupBy> groupBy,
            Optional<Expression> having,
            Optional<OrderBy> orderBy,
            Optional<String> limit,
            Optional<Windowing> windowing) {
        this(Optional.of(location), select, from, where, groupBy, having, orderBy, limit, windowing);
    }

    private QuerySpecification(
            Optional<NodeLocation> location,
            Select select,
            Optional<Relation> from,
            Optional<Expression> where,
            Optional<GroupBy> groupBy,
            Optional<Expression> having,
            Optional<OrderBy> orderBy,
            Optional<String> limit,
            Optional<Windowing> windowing) {
        super(location);
        requireNonNull(select, "select is null");
        requireNonNull(from, "from is null");
        requireNonNull(where, "where is null");
        requireNonNull(groupBy, "groupBy is null");
        requireNonNull(having, "having is null");
        requireNonNull(orderBy, "orderBy is null");
        requireNonNull(limit, "limit is null");
        requireNonNull(windowing, "windowing is null");

        this.select = select;
        this.from = from;
        this.where = where;
        this.groupBy = groupBy;
        this.having = having;
        this.orderBy = orderBy;
        this.limit = limit;
        this.windowing = windowing;
    }

    /**
     * Get the {@link #select} of this QuerySpecification.
     *
     * @return A {@link Select}.
     */
    public Select getSelect() {
        return select;
    }

    /**
     * Get the {@link #from} of this QuerySpecification.
     *
     * @return An Optional of {@link Relation}.
     */
    public Optional<Relation> getFrom() {
        return from;
    }

    /**
     * Get the {@link #where} of this QuerySpecification.
     *
     * @return An Optional of {@link Expression}.
     */
    public Optional<Expression> getWhere() {
        return where;
    }

    /**
     * Get the {@link #groupBy} of this QuerySpecification.
     *
     * @return An Optional of {@link GroupBy}.
     */
    public Optional<GroupBy> getGroupBy() {
        return groupBy;
    }

    /**
     * Get the {@link #having} of this QuerySpecification.
     *
     * @return An Optional of {@link Expression}.
     */
    public Optional<Expression> getHaving() {
        return having;
    }

    /**
     * Get the {@link #orderBy} of this QuerySpecification.
     *
     * @return An Optional of {@link OrderBy}.
     */
    public Optional<OrderBy> getOrderBy() {
        return orderBy;
    }

    /**
     * Get the {@link #limit} of this QuerySpecification.
     *
     * @return An Optional of String.
     */
    public Optional<String> getLimit() {
        return limit;
    }

    /**
     * Get the {@link #windowing} of this QuerySpecification.
     *
     * @return An Optional of {@link Windowing}.
     */
    public Optional<Windowing> getWindowing() {
        return windowing;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitQuerySpecification(this, context);
    }

    @Override
    public List<Node> getChildren() {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.add(select);
        from.ifPresent(nodes::add);
        where.ifPresent(nodes::add);
        groupBy.ifPresent(nodes::add);
        having.ifPresent(nodes::add);
        orderBy.ifPresent(nodes::add);
        windowing.ifPresent(nodes::add);
        return nodes.build();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("select", select)
                .add("from", from)
                .add("where", where.orElse(null))
                .add("groupBy", groupBy)
                .add("having", having.orElse(null))
                .add("orderBy", orderBy)
                .add("limit", limit.orElse(null))
                .add("windowing", windowing.orElse(null))
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

        return hasEqualFields(obj);
    }

    private boolean hasEqualFields(Object obj) {
        QuerySpecification o = (QuerySpecification) obj;
        return Objects.equals(select, o.select) &&
                Objects.equals(from, o.from) &&
                Objects.equals(where, o.where) &&
                Objects.equals(groupBy, o.groupBy) &&
                Objects.equals(having, o.having) &&
                Objects.equals(orderBy, o.orderBy) &&
                Objects.equals(limit, o.limit) &&
                Objects.equals(windowing, o.windowing);
    }

    @Override
    public int hashCode() {
        return Objects.hash(select, from, where, groupBy, having, orderBy, limit, windowing);
    }
}
