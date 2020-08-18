/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.query.aggregations.Distribution;
import com.yahoo.bullet.query.aggregations.DistributionType;
import com.yahoo.bullet.query.aggregations.LinearDistribution;

import java.util.Objects;

public class LinearDistributionNode extends DistributionNode {
    private final int numberOfPoints;

    /**
     * Constructs a LinearDistributionNode from a {@link com.yahoo.bullet.query.aggregations.DistributionType}, {@link ExpressionNode}, and a {@link Long} number of
     * points.
     *
     * @param type The distribution type.
     * @param expression The distribution variable.
     * @param numberOfPoints The number of evenly-spaced points in the returned distribution.
     * @param nodeLocation The location of the node.
     */
    public LinearDistributionNode(DistributionType type, ExpressionNode expression, int numberOfPoints, NodeLocation nodeLocation) {
        super(type, expression, nodeLocation);
        this.numberOfPoints = numberOfPoints;
    }

    @Override
    public Distribution getAggregation(Integer size) {
        return new LinearDistribution(expression.getName(), type, size, numberOfPoints);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof LinearDistributionNode)) {
            return false;
        }
        LinearDistributionNode other = (LinearDistributionNode) obj;
        return type == other.type &&
               Objects.equals(expression, other.expression) &&
               Objects.equals(numberOfPoints, other.numberOfPoints);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, expression, numberOfPoints);
    }

    @Override
    public String toString() {
        return type.getName() + "(" + expression.getName() + ", LINEAR, " + numberOfPoints + ")";
    }
}
