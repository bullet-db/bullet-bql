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
import com.yahoo.bullet.typesystem.Schema;
import com.yahoo.bullet.typesystem.Type;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

@RequiredArgsConstructor
public class OrderByProcessor extends DefaultTraversalVisitor<Void, LayeredSchema> {
    private final Schema baseSchema;
    private Set<String> additionalFields = new HashSet<>();

    @Override
    public Void process(Node node) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    public Void process(Node node, LayeredSchema layeredSchema) {
        if (layeredSchema.hasField(((ExpressionNode) node).getName())) {
            return null;
        }
        return super.process(node, layeredSchema);
    }

    public Set<String> process(Collection<? extends Node> nodes, LayeredSchema layeredSchema) {
        nodes.forEach(node -> process(node, layeredSchema));
        return additionalFields;
    }

    @Override
    protected Void visitExpression(ExpressionNode node, LayeredSchema layeredSchema) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected Void visitFieldExpression(FieldExpressionNode node, LayeredSchema layeredSchema) {
        if (baseSchema != null) {
            String name = node.getField().getValue();
            Type type = baseSchema.getType(name);
            if (type != Type.NULL) {
                layeredSchema.getSchema().addField(name, type);
                additionalFields.add(name);
            }
        }
        return null;
    }

    @Override
    protected Void visitLiteral(LiteralNode node, LayeredSchema layeredSchema) {
        return null;
    }

    public static Set<String> visit(Collection<? extends Node> nodes, LayeredSchema layeredSchema, Schema baseSchema) {
        return new OrderByProcessor(baseSchema).process(nodes, layeredSchema);
    }
}
