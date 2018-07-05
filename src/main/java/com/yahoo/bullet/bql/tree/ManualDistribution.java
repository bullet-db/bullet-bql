/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.base.Joiner;
import com.yahoo.bullet.aggregations.Distribution.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.yahoo.bullet.aggregations.Distribution.POINTS;
import static com.yahoo.bullet.aggregations.Distribution.TYPE;
import static java.util.Objects.requireNonNull;

public class ManualDistribution extends Distribution {
    private final List<Double> points;

    /**
     * Constructor that requires a List of {@link Expression} columns, a {@link Type} and a List of Double points.
     *
     * @param columns A List of {@link Expression}.
     * @param type    A {@link Type}.
     * @param points  A List of Double.
     */
    public ManualDistribution(List<Expression> columns, Type type, List<Double> points) {
        this(Optional.empty(), columns, type, points);
    }

    /**
     * Constructor that requires a {@link NodeLocation}, a List of {@link Expression} columns, a {@link Type} and a List of Double points.
     *
     * @param location A {@link NodeLocation}.
     * @param columns  A List of {@link Expression}.
     * @param type     A {@link Type}.
     * @param points   A List of Double.
     */
    public ManualDistribution(NodeLocation location, List<Expression> columns, Type type, List<Double> points) {
        this(Optional.of(location), columns, type, points);
    }

    private ManualDistribution(Optional<NodeLocation> location, List<Expression> columns, Type type, List<Double> points) {
        super(location, columns, type);
        this.points = requireNonNull(points, "points is null");
    }

    @Override
    public Map<String, Object> getAttributes() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(TYPE, type);
        attributes.put(POINTS, points);
        return attributes;
    }

    @Override
    public String attributesToString() {
        StringBuilder builder = new StringBuilder();
        builder.append(getDistributionType(type))
                .append("(")
                .append(columns.get(0))
                .append(", MANUAL, ")
                .append(Joiner.on(", ").join(points))
                .append(")");
        return builder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ManualDistribution that = (ManualDistribution) o;
        return Objects.equals(columns, that.columns) && Objects.equals(type, that.type) && Objects.equals(points, that.points);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns, type, points);
    }
}
