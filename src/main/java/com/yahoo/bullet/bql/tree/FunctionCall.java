/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/FunctionCall.java
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;
import com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType;
import com.yahoo.bullet.bql.tree.SelectItem.Type;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class FunctionCall extends Expression {
    private final GroupOperationType type;
    private final Optional<Expression> filter;
    private final Optional<OrderBy> orderBy;
    private final boolean distinct;
    private final List<Expression> arguments;

    /**
     * Constructor that requires a {@link GroupOperationType} and a List of {@link Expression} arguments.
     *
     * @param type      A {@link GroupOperationType}.
     * @param arguments A List of {@link Expression} arguments.
     */
    public FunctionCall(GroupOperationType type, List<Expression> arguments) {
        this(Optional.empty(), type, Optional.empty(), Optional.empty(), false, arguments);
    }

    /**
     * Constructor that requires a {@link GroupOperationType}, an Optional of {@link Expression} filter,
     * an Optional of {@link OrderBy} orderBy, a boolean distinct and a List of {@link Expression} arguments.
     *
     * @param type      A {@link GroupOperationType}.
     * @param filter    An Optional of {@link Expression} filter.
     * @param orderBy   An Optional of {@link OrderBy} orderBy.
     * @param distinct  A boolean distinct.
     * @param arguments A List of {@link Expression} arguments.
     */
    public FunctionCall(GroupOperationType type, Optional<Expression> filter, Optional<OrderBy> orderBy, boolean distinct, List<Expression> arguments) {
        this(Optional.empty(), type, filter, orderBy, distinct, arguments);
    }

    /**
     * Constructor that requires a {@link NodeLocation}, a {@link GroupOperationType}, an Optional of {@link Expression} filter,
     * an Optional of {@link OrderBy} orderBy, a boolean distinct and a List of {@link Expression} arguments.
     *
     * @param location  A {@link NodeLocation}.
     * @param type      A {@link GroupOperationType}.
     * @param filter    An Optional of {@link Expression} filter.
     * @param orderBy   An Optional of {@link OrderBy} orderBy.
     * @param distinct  A boolean distinct.
     * @param arguments A List of {@link Expression} arguments.
     */
    public FunctionCall(NodeLocation location, GroupOperationType type, Optional<Expression> filter, Optional<OrderBy> orderBy, boolean distinct, List<Expression> arguments) {
        this(Optional.of(location), type, filter, orderBy, distinct, arguments);
    }

    private FunctionCall(Optional<NodeLocation> location, GroupOperationType type, Optional<Expression> filter, Optional<OrderBy> orderBy, boolean distinct, List<Expression> arguments) {
        super(location);
        requireNonNull(type, "name is null");
        requireNonNull(filter, "Filter is null");
        requireNonNull(orderBy, "orderBy is null");
        requireNonNull(arguments, "arguments is null");

        this.type = type;
        this.filter = filter;
        this.orderBy = orderBy;
        this.distinct = distinct;
        this.arguments = arguments;
    }

    /**
     * Get the {@link #type} of this FunctionCall.
     *
     * @return A {@link GroupOperationType}.
     */
    public GroupOperationType getType() {
        return type;
    }

    /**
     * Get the {@link Type} of this FunctionCall.
     *
     * @param clazz {@link Type} class.
     * @return The {@link Type}.
     */
    public Type getType(Class<SelectItem.Type> clazz) {
        if (type.toString().equalsIgnoreCase("count") && distinct) {
            return Type.COUNT_DISTINCT;
        }

        return Type.GROUP;
    }

    /**
     * Get the {@link #orderBy} of this FunctionCall.
     *
     * @return An Optional of {@link OrderBy}.
     */
    public Optional<OrderBy> getOrderBy() {
        return orderBy;
    }

    /**
     * Get the {@link #distinct} of this FunctionCall.
     *
     * @return A boolean distinct.
     */
    public boolean isDistinct() {
        return distinct;
    }

    /**
     * Get the {@link #arguments} of this FunctionCall.
     *
     * @return A List of {@link Expression} arguments.
     */
    public List<Expression> getArguments() {
        return arguments;
    }

    /**
     * Get the {@link #filter} of this FunctionCall.
     *
     * @return An Optional of {@link Expression} filter.
     */
    public Optional<Expression> getFilter() {
        return filter;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitFunctionCall(this, context);
    }

    @Override
    public List<Node> getChildren() {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        filter.ifPresent(nodes::add);
        orderBy.map(OrderBy::getSortItems).map(nodes::addAll);
        nodes.addAll(arguments);
        return nodes.build();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        FunctionCall o = (FunctionCall) obj;
        return Objects.equals(type, o.type) &&
                Objects.equals(filter, o.filter) &&
                Objects.equals(orderBy, o.orderBy) &&
                Objects.equals(distinct, o.distinct) &&
                Objects.equals(arguments, o.arguments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, distinct, filter, orderBy, arguments);
    }
}
