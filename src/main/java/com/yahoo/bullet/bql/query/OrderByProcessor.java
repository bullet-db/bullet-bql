/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.bql.tree.DefaultTraversalVisitor;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.FieldExpressionNode;
import com.yahoo.bullet.bql.tree.LiteralNode;
import com.yahoo.bullet.bql.tree.Node;
import lombok.AllArgsConstructor;

import java.util.Collection;

@AllArgsConstructor
public class OrderByProcessor extends DefaultTraversalVisitor<Void, ProcessedQuery> {
    private static final OrderByProcessor INSTANCE = new OrderByProcessor();

    @Override
    public Void process(Node node) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    public Void process(Node node, ProcessedQuery context) {
        if (context.getPostAggregationMapping().containsKey(node)) {
            return null;
        }
        return super.process(node, context);
    }

    public Void process(Collection<? extends Node> nodes, ProcessedQuery context) {
        nodes.forEach(node -> process(node, context));
        return null;
    }

    @Override
    protected Void visitExpression(ExpressionNode node, ProcessedQuery context) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected Void visitFieldExpression(FieldExpressionNode node, ProcessedQuery context) {
        if (!context.getSelectNames().contains(node.getField().getValue())) {
            context.getOrderByExtraSelectNodes().add(node);
        }
        return null;
    }

    @Override
    protected Void visitLiteral(LiteralNode node, ProcessedQuery context) {
        return null;
    }

    public static void visit(Collection<ExpressionNode> nodes, ProcessedQuery processedQuery) {
        INSTANCE.process(nodes, processedQuery);
    }
}
