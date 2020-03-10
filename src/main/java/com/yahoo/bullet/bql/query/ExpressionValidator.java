/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.tree.BinaryExpressionNode;
import com.yahoo.bullet.bql.tree.CastExpressionNode;
import com.yahoo.bullet.bql.tree.CountDistinctNode;
import com.yahoo.bullet.bql.tree.DefaultTraversalVisitor;
import com.yahoo.bullet.bql.tree.DistributionNode;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.FieldExpressionNode;
import com.yahoo.bullet.bql.tree.GroupOperationNode;
import com.yahoo.bullet.bql.tree.IdentifierNode;
import com.yahoo.bullet.bql.tree.ListExpressionNode;
import com.yahoo.bullet.bql.tree.LiteralNode;
import com.yahoo.bullet.bql.tree.NAryExpressionNode;
import com.yahoo.bullet.bql.tree.Node;
import com.yahoo.bullet.bql.tree.NullPredicateNode;
import com.yahoo.bullet.bql.tree.ParenthesesExpressionNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.bql.tree.UnaryExpressionNode;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.parsing.expressions.Expression;
import com.yahoo.bullet.parsing.expressions.FieldExpression;
import com.yahoo.bullet.parsing.expressions.Operation;
import com.yahoo.bullet.parsing.expressions.ValueExpression;
import com.yahoo.bullet.typesystem.Type;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Sets and returns type for visited expression. Accumulates errors found in processed query.
 *
 */
@AllArgsConstructor
public class ExpressionValidator extends DefaultTraversalVisitor<Type, LayeredSchema> {
    private final ProcessedQuery processedQuery;
    @Getter @Setter
    private Map<ExpressionNode, Expression> mapping;

    @Override
    public Type process(Node node, LayeredSchema schema) {
        //Expression expression = processedQuery.getExpression((ExpressionNode) node);
        Expression expression = mapping.get((ExpressionNode) node);
        // expression can be null if the node is a top k or distribution node
        if (expression != null) {
            if (expression.getType() != null) {
                return expression.getType();
            }
            if (expression instanceof FieldExpression) {
                FieldExpression fieldExpression = (FieldExpression) expression;
                FieldExpressionNode temp = new FieldExpressionNode(new IdentifierNode(fieldExpression.getField(), false), fieldExpression.getIndex(), fieldExpression.getKey() != null ? new IdentifierNode(fieldExpression.getKey(), false) : null, fieldExpression.getSubKey() != null ? new IdentifierNode(fieldExpression.getSubKey(), false) : null, null);
                Type fieldType = schema.getType(fieldExpression.getField());
                Optional<List<BulletError>> errors = TypeChecker.validateFieldType(temp, fieldType, temp.hasIndexOrKey(), temp.hasSubKey());
                if (errors.isPresent()) {
                    processedQuery.getErrors().addAll(errors.get());
                    return setType((ExpressionNode) node, Type.UNKNOWN);
                }
                return setType((ExpressionNode) node, TypeChecker.getFieldType(fieldType, temp.hasIndexOrKey(), temp.hasSubKey()));
            }
        }
        return super.process(node, schema);
    }

    public List<Type> process(Collection<? extends Node> nodes, LayeredSchema schema) {
        return nodes.stream().map(node -> process(node, schema)).collect(Collectors.toList());
    }

    @Override
    protected Type visitFieldExpression(FieldExpressionNode node, LayeredSchema schema) {
        Type fieldType = schema.getType(node.getField().getValue());
        Optional<List<BulletError>> errors = TypeChecker.validateFieldType(node, fieldType, node.hasIndexOrKey(), node.hasSubKey());
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN);
        }
        return setType(node, TypeChecker.getFieldType(fieldType, node.hasIndexOrKey(), node.hasSubKey()));
    }

    @Override
    protected Type visitListExpression(ListExpressionNode node, LayeredSchema schema) {
        List<Type> argTypes = node.getExpressions().stream().map(processFunc(schema)).collect(Collectors.toList());
        Optional<List<BulletError>> errors = TypeChecker.validateListTypes(node, argTypes);
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN);
        }
        return setType(node, TypeChecker.getListType(argTypes));
    }

    @Override
    protected Type visitNullPredicate(NullPredicateNode node, LayeredSchema schema) {
        Type argType = process(node.getExpression(), schema);
        Operation op = node.isNot() ? Operation.IS_NOT_NULL : Operation.IS_NULL;
        Optional<List<BulletError>> errors = TypeChecker.validateUnaryType(null, argType, op);
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN);
        }
        return setType(node, TypeChecker.getUnaryType(op));
    }

    @Override
    protected Type visitUnaryExpression(UnaryExpressionNode node, LayeredSchema schema) {
        Type argType = process(node.getExpression(), schema);
        Optional<List<BulletError>> errors = TypeChecker.validateUnaryType(node, argType, node.getOp());
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN);
        }
        return setType(node, TypeChecker.getUnaryType(node.getOp()));
    }

    @Override
    protected Type visitNAryExpression(NAryExpressionNode node, LayeredSchema schema) {
        List<Type> argTypes = node.getExpressions().stream().map(processFunc(schema)).collect(Collectors.toList());
        Optional<List<BulletError>> errors = TypeChecker.validateNAryType(node, argTypes, node.getOp());
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN);
        }
        return setType(node, TypeChecker.getNAryType(argTypes, node.getOp()));
    }

    @Override
    protected Type visitGroupOperation(GroupOperationNode node, LayeredSchema schema) {
        if (node.getOp() == GroupOperation.GroupOperationType.COUNT) {
            return setType(node, Type.LONG);
        }
        Type argType = process(node.getExpression(), schema);
        Optional<List<BulletError>> errors = TypeChecker.validateNumericType(node, argType);
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN);
        }
        return setType(node, TypeChecker.getAggregateType(argType, node.getOp()));
    }

    @Override
    protected Type visitCountDistinct(CountDistinctNode node, LayeredSchema schema) {
        List<Type> argTypes = node.getExpressions().stream().map(processFunc(schema)).collect(Collectors.toList());
        Optional<List<BulletError>> errors = TypeChecker.validateKnownTypes(argTypes);
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN);
        }
        return setType(node, Type.LONG);
    }

    @Override
    protected Type visitDistribution(DistributionNode node, LayeredSchema schema) {
        Type argType = process(node.getExpression(), schema);
        TypeChecker.validateNumericType(node, argType).ifPresent(errors -> processedQuery.getErrors().addAll(errors));
        return Type.UNKNOWN;
    }

    @Override
    protected Type visitTopK(TopKNode node, LayeredSchema schema) {
        List<Type> argTypes = node.getExpressions().stream().map(processFunc(schema)).collect(Collectors.toList());
        TypeChecker.validateKnownTypes(argTypes).ifPresent(errors -> processedQuery.getErrors().addAll(errors));
        return Type.UNKNOWN;
    }

    @Override
    protected Type visitCastExpression(CastExpressionNode node, LayeredSchema schema) {
        Type argType = process(node.getExpression(), schema);
        Optional<List<BulletError>> errors = TypeChecker.validateCastType(node, argType, node.getCastType());
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN);
        }
        return setType(node, node.getCastType());
    }

    @Override
    protected Type visitBinaryExpression(BinaryExpressionNode node, LayeredSchema schema) {
        Type leftType = process(node.getLeft(), schema);
        Type rightType = process(node.getRight(), schema);
        Optional<List<BulletError>> errors = TypeChecker.validateBinaryType(node, leftType, rightType, node.getOp());
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN);
        }
        return setType(node, TypeChecker.getBinaryType(leftType, rightType, node.getOp()));
    }

    @Override
    protected Type visitParenthesesExpression(ParenthesesExpressionNode node, LayeredSchema schema) {
        Type type = process(node.getExpression(), schema);
        return setType(node, type);
    }

    @Override
    protected Type visitLiteral(LiteralNode node, LayeredSchema schema) {
        // This shouldn't be called since literals have a type to begin with.
        //throw new ParsingException("Literal missing its type.");
        return setType(node, new ValueExpression(node.getValue()).getType());
    }

    private Type setType(ExpressionNode node, Type type) {
        mapping.computeIfAbsent(node, k -> new FieldExpression("")).setType(type);
        //processedQuery.getExpression(node).setType(type);
        return type;
    }
}
