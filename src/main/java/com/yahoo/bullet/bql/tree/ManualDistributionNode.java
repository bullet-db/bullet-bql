/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.query.aggregations.Distribution;
import com.yahoo.bullet.query.aggregations.DistributionType;
import com.yahoo.bullet.query.aggregations.ManualDistribution;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ManualDistributionNode extends DistributionNode {
    private final List<Double> points;

    /**
     * Constructor that requires a List of {@link ExpressionNode} columns, a {@link DistributionType} and a List of Double points.
     *
     * @param type A {@link DistributionType}.
     * @param expression An {@link ExpressionNode}.
     * @param points A List of Double.
     * @param nodeLocation The location of the node.
     */
    public ManualDistributionNode(DistributionType type, ExpressionNode expression, List<Double> points, NodeLocation nodeLocation) {
        super(type, expression, nodeLocation);
        this.points = points;
    }

    @Override
    public Distribution getAggregation(Integer size) {
        return new ManualDistribution(expression.getName(), type, size, points);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
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

    @Override
    public String toString() {
        return type.getName() + "(" + expression.getName() + ", MANUAL, " + points.stream().map(Object::toString).collect(Collectors.joining(", ")) + ")";
    }
}
