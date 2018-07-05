/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.aggregations.Distribution.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.yahoo.bullet.aggregations.Distribution.NUMBER_OF_POINTS;
import static com.yahoo.bullet.aggregations.Distribution.TYPE;
import static java.util.Objects.requireNonNull;

public class LinearDistribution extends Distribution {
    private final Long numberOfPoints;

    /**
     * Constructor that requires a List of {@link Expression} columns, a {@link Type} and numberOfPoints.
     *
     * @param columns        A List of {@link Expression}.
     * @param type           A {@link Type}.
     * @param numberOfPoints A Long.
     */
    public LinearDistribution(List<Expression> columns, Type type, Long numberOfPoints) {
        this(Optional.empty(), columns, type, numberOfPoints);
    }

    /**
     * Constructor that requires a {@link NodeLocation}, a List of {@link Expression} columns, a {@link Type} and numberOfPoints.
     *
     * @param location       A {@link NodeLocation}.
     * @param columns        A List of {@link Expression}.
     * @param type           A {@link Type}.
     * @param numberOfPoints A Long.
     */
    public LinearDistribution(NodeLocation location, List<Expression> columns, Type type, Long numberOfPoints) {
        this(Optional.of(location), columns, type, numberOfPoints);
    }

    private LinearDistribution(Optional<NodeLocation> location, List<Expression> columns, Type type, Long numberOfPoints) {
        super(location, columns, type);
        this.numberOfPoints = requireNonNull(numberOfPoints, "numberOfPoints is null");
    }

    @Override
    public Map<String, Object> getAttributes() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(TYPE, type);
        attributes.put(NUMBER_OF_POINTS, numberOfPoints);
        return attributes;
    }

    @Override
    public String attributesToString() {
        StringBuilder builder = new StringBuilder();

        builder.append(getDistributionType(type))
                .append("(")
                .append(columns.get(0).toFormatlessString())
                .append(", LINEAR, ")
                .append(numberOfPoints)
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

        LinearDistribution that = (LinearDistribution) o;
        return Objects.equals(columns, that.columns) && Objects.equals(type, that.type) && Objects.equals(numberOfPoints, that.numberOfPoints);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns, type, numberOfPoints);
    }
}
