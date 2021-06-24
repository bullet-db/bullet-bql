/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.query.aggregations.Distribution;
import com.yahoo.bullet.query.aggregations.DistributionType;
import lombok.Getter;

import java.util.Collections;
import java.util.List;

@Getter
public abstract class DistributionNode extends ExpressionNode {
    protected final DistributionType type;
    protected final ExpressionNode expression;

    protected DistributionNode(DistributionType type, ExpressionNode expression, NodeLocation nodeLocation) {
        super(nodeLocation);
        this.type = type;
        this.expression = expression;
    }

    public abstract Distribution getAggregation(Integer size);

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitDistribution(this, context);
    }

    @Override
    public List<ExpressionNode> getChildren() {
        return Collections.singletonList(expression);
    }
}
