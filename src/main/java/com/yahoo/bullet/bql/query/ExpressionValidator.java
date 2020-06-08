/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.tree.BinaryExpressionNode;
import com.yahoo.bullet.bql.tree.CastExpressionNode;
import com.yahoo.bullet.bql.tree.CountDistinctNode;
import com.yahoo.bullet.bql.tree.DefaultTraversalVisitor;
import com.yahoo.bullet.bql.tree.DistributionNode;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.FieldExpressionNode;
import com.yahoo.bullet.bql.tree.GroupOperationNode;
import com.yahoo.bullet.bql.tree.ListExpressionNode;
import com.yahoo.bullet.bql.tree.LiteralNode;
import com.yahoo.bullet.bql.tree.NAryExpressionNode;
import com.yahoo.bullet.bql.tree.Node;
import com.yahoo.bullet.bql.tree.NullPredicateNode;
import com.yahoo.bullet.bql.tree.ParenthesesExpressionNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.bql.tree.UnaryExpressionNode;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.typesystem.Type;
import lombok.AllArgsConstructor;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Sets and returns type for visited expression. Accumulates errors found in processed query.
 *
 */
@AllArgsConstructor
public class ExpressionValidator extends DefaultTraversalVisitor<Type, Map<ExpressionNode, Expression>> {
    /**
     * Placeholder expression for just type information.
     */
    private static class TypeExpression extends Expression {
        @Override
        public Evaluator getEvaluator() {
            return null;
        }

        @Override
        public boolean equals(Object obj) {
            return false;
        }

        @Override
        public int hashCode() {
            return 0;
        }
    }

    private final ProcessedQuery processedQuery;
    private final LayeredSchema schema;

    @Override
    public Type process(Node node, Map<ExpressionNode, Expression> mapping) {
        Expression expression = mapping.get(node);
        if (expression != null) {
            if (expression.getType() != null) {
                return expression.getType();
            }
            // Aggregate (i.e. not a FieldExpressionNode) to field expression mapping; guaranteed to be a simple field that exists in the schema
            if (expression instanceof FieldExpression) {
                FieldExpression fieldExpression = (FieldExpression) expression;
                Type type = schema.getType(fieldExpression.getField());
                if (!Type.isNull(type) && fieldExpression.getIndex() == null && fieldExpression.getKey() == null) {
                    return setType((ExpressionNode) node, type, mapping);
                }
            }
        }
        return super.process(node, mapping);
    }

    public List<Type> process(Collection<? extends Node> nodes, Map<ExpressionNode, Expression> mapping) {
        return nodes.stream().map(node -> process(node, mapping)).collect(Collectors.toList());
    }

    @Override
    protected Type visitFieldExpression(FieldExpressionNode node, Map<ExpressionNode, Expression> mapping) {
        Type type = schema.getType(node.getField().getValue());
        Optional<List<BulletError>> errors = TypeChecker.validateFieldType(node, type, node.hasIndexOrKey(), node.hasSubKey());
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN, mapping);
        }
        return setType(node, TypeChecker.getFieldType(type, node.hasIndexOrKey(), node.hasSubKey()), mapping);
    }

    @Override
    protected Type visitListExpression(ListExpressionNode node, Map<ExpressionNode, Expression> mapping) {
        Set<Type> argTypes = node.getExpressions().stream().map(processFunc(mapping)).collect(Collectors.toSet());
        Optional<List<BulletError>> errors = TypeChecker.validateListTypes(node, argTypes);
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN, mapping);
        }
        return setType(node, TypeChecker.getListType(argTypes), mapping);
    }

    @Override
    protected Type visitNullPredicate(NullPredicateNode node, Map<ExpressionNode, Expression> mapping) {
        Type argType = process(node.getExpression(), mapping);
        Operation op = node.isNot() ? Operation.IS_NOT_NULL : Operation.IS_NULL;
        Optional<List<BulletError>> errors = TypeChecker.validateUnaryType(null, argType, op);
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN, mapping);
        }
        return setType(node, TypeChecker.getUnaryType(op), mapping);
    }

    @Override
    protected Type visitUnaryExpression(UnaryExpressionNode node, Map<ExpressionNode, Expression> mapping) {
        Type argType = process(node.getExpression(), mapping);
        Optional<List<BulletError>> errors = TypeChecker.validateUnaryType(node, argType, node.getOp());
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN, mapping);
        }
        return setType(node, TypeChecker.getUnaryType(node.getOp()), mapping);
    }

    @Override
    protected Type visitNAryExpression(NAryExpressionNode node, Map<ExpressionNode, Expression> mapping) {
        List<Type> argTypes = node.getExpressions().stream().map(processFunc(mapping)).collect(Collectors.toList());
        Optional<List<BulletError>> errors = TypeChecker.validateNAryType(node, argTypes, node.getOp());
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN, mapping);
        }
        return setType(node, TypeChecker.getNAryType(argTypes, node.getOp()), mapping);
    }

    @Override
    protected Type visitGroupOperation(GroupOperationNode node, Map<ExpressionNode, Expression> mapping) {
        if (node.getOp() == GroupOperation.GroupOperationType.COUNT) {
            return setType(node, Type.LONG, mapping);
        }
        Type argType = process(node.getExpression(), mapping);
        Optional<List<BulletError>> errors = TypeChecker.validateNumericType(node, argType);
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN, mapping);
        }
        return setType(node, TypeChecker.getAggregateType(argType, node.getOp()), mapping);
    }

    @Override
    protected Type visitCountDistinct(CountDistinctNode node, Map<ExpressionNode, Expression> mapping) {
        List<Type> argTypes = node.getExpressions().stream().map(processFunc(mapping)).collect(Collectors.toList());
        Optional<List<BulletError>> errors = TypeChecker.validatePrimitiveTypes(node, argTypes);
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN, mapping);
        }
        return setType(node, Type.LONG, mapping);
    }

    @Override
    protected Type visitDistribution(DistributionNode node, Map<ExpressionNode, Expression> mapping) {
        Type argType = process(node.getExpression(), mapping);
        TypeChecker.validateNumericType(node, argType).ifPresent(errors -> processedQuery.getErrors().addAll(errors));
        return Type.UNKNOWN;
    }

    @Override
    protected Type visitTopK(TopKNode node, Map<ExpressionNode, Expression> mapping) {
        List<Type> argTypes = node.getExpressions().stream().map(processFunc(mapping)).collect(Collectors.toList());
        TypeChecker.validatePrimitiveTypes(node, argTypes).ifPresent(errors -> processedQuery.getErrors().addAll(errors));
        return Type.UNKNOWN;
    }

    @Override
    protected Type visitCastExpression(CastExpressionNode node, Map<ExpressionNode, Expression> mapping) {
        Type argType = process(node.getExpression(), mapping);
        Optional<List<BulletError>> errors = TypeChecker.validateCastType(node, argType, node.getCastType());
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN, mapping);
        }
        return setType(node, node.getCastType(), mapping);
    }

    @Override
    protected Type visitBinaryExpression(BinaryExpressionNode node, Map<ExpressionNode, Expression> mapping) {
        Type leftType = process(node.getLeft(), mapping);
        Type rightType = process(node.getRight(), mapping);
        Optional<List<BulletError>> errors = TypeChecker.validateBinaryType(node, leftType, rightType, node.getOp());
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN, mapping);
        }
        return setType(node, TypeChecker.getBinaryType(leftType, rightType, node.getOp()), mapping);
    }

    @Override
    protected Type visitParenthesesExpression(ParenthesesExpressionNode node, Map<ExpressionNode, Expression> mapping) {
        Type type = process(node.getExpression(), mapping);
        return setType(node, type, mapping);
    }

    @Override
    protected Type visitLiteral(LiteralNode node, Map<ExpressionNode, Expression> mapping) {
        // This shouldn't be called since literals have a type to begin with.
        throw new ParsingException("Literal missing its type.");
    }

    private Type setType(ExpressionNode node, Type type, Map<ExpressionNode, Expression> mapping) {
        // An expression is absent if it does not exist in the projection or computation tree.
        mapping.computeIfAbsent(node, k -> new TypeExpression()).setType(type);
        return type;
    }
}
