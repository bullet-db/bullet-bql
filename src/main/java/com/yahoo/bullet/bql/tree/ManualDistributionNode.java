/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.aggregations.Distribution.Type;
import lombok.Getter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.yahoo.bullet.aggregations.Distribution.POINTS;
import static com.yahoo.bullet.aggregations.Distribution.TYPE;
import static java.util.Objects.requireNonNull;

@Getter
public class ManualDistributionNode extends DistributionNode {
    private final List<Double> points;

    /**
     * Constructor that requires a List of {@link ExpressionNode} columns, a {@link Type} and a List of Double points.
     *
     * @param type    A {@link Type}.
     * @param expression An {@link ExpressionNode}.
     * @param points  A List of Double.
     */
    public ManualDistributionNode(Type type, ExpressionNode expression, List<Double> points) {
        super(type, expression);
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
        return getDistributionType(type) + "(" + expression.toFormatlessString() + ", MANUAL, " +
                points.stream().map(Object::toString).collect(Collectors.joining(", ")) + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ManualDistributionNode)) {
            return false;
        }
        ManualDistributionNode other = (ManualDistributionNode) obj;
        return type == other.type &&
               Objects.equals(expression, other.expression) &&
               Objects.equals(points, other.points);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, expression, points);
    }
}
