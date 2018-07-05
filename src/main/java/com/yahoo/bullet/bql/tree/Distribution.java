/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;
import com.yahoo.bullet.aggregations.Distribution.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public abstract class Distribution extends Expression {
    protected final List<Expression> columns;
    protected final Type type;

    /**
     * Constructor that requires a {@link NodeLocation}, a List of {@link Expression} columns and a {@link Type}.
     *
     * @param location A {@link NodeLocation}.
     * @param columns  A List of {@link Expression}.
     * @param type     A {@link Type}.
     */
    protected Distribution(Optional<NodeLocation> location, List<Expression> columns, Type type) {
        super(location);
        this.columns = requireNonNull(columns, "columns is null");
        this.type = requireNonNull(type, "type is null");
    }

    /**
     * Get the {@link #columns} of this Distribution.
     *
     * @return A List of {@link Expression}.
     */
    public List<Expression> getColumns() {
        return columns;
    }

    /**
     * Get the {@link #type} of this Distribution.
     *
     * @return A {@link Type}.
     */
    public Type getType() {
        return type;
    }

    /**
     * Get the attributes of this Distribution.
     *
     * @return A Map of String and Object that represents attributes.
     */
    public abstract Map<String, Object> getAttributes();

    /**
     * Convert attributes to BQL expression.
     *
     * @return A String.
     */
    public abstract String attributesToString();

    /**
     * Get the BQL String of this Distribution's type.
     *
     * @param type A {@link Type}.
     * @return A BQL String that represents type.
     * @throws UnsupportedOperationException when Distribution's type is not supported.
     */
    protected String getDistributionType(Type type) throws UnsupportedOperationException {
        switch (type) {
            case QUANTILE:
                return "QUANTILE";
            case PMF:
                return "FREQ";
            case CDF:
                return "CUMFREQ";
        }

        throw new UnsupportedOperationException("Distribution type is not supported");
    }

    @Override
    public SelectItem.Type getType(Class<SelectItem.Type> clazz) {
        return SelectItem.Type.DISTRIBUTION;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitDistribution(this, context);
    }

    @Override
    public List<Node> getChildren() {
        return ImmutableList.of();
    }
}
