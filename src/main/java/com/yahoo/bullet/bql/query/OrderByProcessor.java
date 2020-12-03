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
import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.typesystem.Type;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class OrderByProcessor extends DefaultTraversalVisitor<Void, QuerySchema> {
    private Set<String> transientFields = new HashSet<>();

    @Override
    public Void process(Node node) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    public Void process(Node node, QuerySchema querySchema) {
        if (querySchema.contains((ExpressionNode) node)) {
            return null;
        }
        return super.process(node, querySchema);
    }

    public Set<String> process(Collection<? extends Node> nodes, QuerySchema querySchema) {
        nodes.forEach(node -> process(node, querySchema));
        return transientFields;
    }

    @Override
    protected Void visitExpression(ExpressionNode node, QuerySchema querySchema) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected Void visitFieldExpression(FieldExpressionNode node, QuerySchema querySchema) {
        String name = node.getField().getValue();
        Type type = querySchema.getBaseSchemaType(name);
        if (type != Type.NULL) {
            FieldExpression expression = new FieldExpression(name);
            expression.setType(type);
            querySchema.addProjectionField(name, expression);
            querySchema.addCurrentSchemaField(name, type);
            transientFields.add(name);
        }
        return null;
    }

    @Override
    protected Void visitLiteral(LiteralNode node, QuerySchema querySchema) {
        return null;
    }

    public static Set<String> visit(Collection<? extends Node> nodes, QuerySchema querySchema) {
        return new OrderByProcessor().process(nodes, querySchema);
    }
}
