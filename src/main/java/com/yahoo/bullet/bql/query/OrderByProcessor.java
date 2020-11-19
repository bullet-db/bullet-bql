/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.bql.temp.QuerySchema;
import com.yahoo.bullet.bql.tree.DefaultTraversalVisitor;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.FieldExpressionNode;
import com.yahoo.bullet.bql.tree.LiteralNode;
import com.yahoo.bullet.bql.tree.Node;
import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

@Getter
public class OrderByProcessor extends DefaultTraversalVisitor<Void, QuerySchema> {
    //private static final OrderByProcessor INSTANCE = new OrderByProcessor();

    private Set<String> transientFields = new HashSet<>();

    @Override
    public Void process(Node node) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    public Void process(Node node, QuerySchema querySchema) {
        if (querySchema.get((ExpressionNode) node) != null) {
            return null;
        }
        return super.process(node, querySchema);
    }

    public Void process(Collection<? extends Node> nodes, QuerySchema querySchema) {
        nodes.forEach(node -> process(node, querySchema));
        return null;
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
            querySchema.addCurrentProjectionField(name, node, type);
            transientFields.add(name);
        }
        return null;
    }

    @Override
    protected Void visitLiteral(LiteralNode node, QuerySchema querySchema) {
        return null;
    }
/*
    public static void visit(Node node, QuerySchema querySchema) {
        INSTANCE.process(node, querySchema);
    }

    public static void visit(Collection<ExpressionNode> nodes, QuerySchema querySchema) {
        INSTANCE.process(nodes, querySchema);
    }
*/
}
