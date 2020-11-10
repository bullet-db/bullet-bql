/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.temp.QuerySchema;
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
import com.yahoo.bullet.bql.tree.SubFieldExpressionNode;
import com.yahoo.bullet.bql.tree.SubSubFieldExpressionNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.bql.tree.UnaryExpressionNode;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.typesystem.Type;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Sets and returns type for visited expression. Accumulates errors found in processed query.
 *
 */
@RequiredArgsConstructor
public class ExpressionValidator extends DefaultTraversalVisitor<Type, QuerySchema> {
    private static final ExpressionValidator INSTANCE = new ExpressionValidator();

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

    //private final ProcessedQuery processedQuery;
    //private final LayeredSchema schema;
    //@Setter
    //private Set<String> aliases;

    @Override
    public Type process(Node node, QuerySchema schema) {
        // if schema.contains(node) then we good hmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm
        Expression expression = schema.getSchemaMapping(node);
        if (expression != null && expression.getType() != null) {
            return expression.getType();
        }
        return super.process(node, schema);
    }

    public List<Type> process(Collection<? extends Node> nodes, QuerySchema schema) {
        if (nodes == null) {
            return null;
        }
        return nodes.stream().map(node -> process(node, schema)).collect(Collectors.toList());
    }

    @Override
    protected Type visitFieldExpression(FieldExpressionNode node, QuerySchema schema) {
        //FieldExpression expression = (FieldExpression) mapping.get(node);
        //String field = expression != null ? expression.getField() : node.getField().getValue();
        //Type type = aliases == null || aliases.contains(field) ? schema.getType(field) : Type.NULL;
        Type type = schema.getType(field);
        Optional<List<BulletError>> errors = TypeChecker.validateFieldType(node, type, node.hasIndexOrKey(), node.hasSubKey());
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN, mapping);
        }
        return setType(node, TypeChecker.getFieldType(type, node.hasIndexOrKey(), node.hasSubKey()), mapping);
    }

    @Override
    protected Type visitSubFieldExpression(SubFieldExpressionNode node, QuerySchema schema) {

    }

    @Override
    protected Type visitSubSubFieldExpression(SubSubFieldExpressionNode node, QuerySchema schema) {
        FieldExpression expression = (FieldExpression) mapping.get(node);


        node.getSubField();


    }

    @Override
    protected Type visitListExpression(ListExpressionNode node, QuerySchema schema) {
        Set<Type> argTypes = node.getExpressions().stream().map(processFunc(mapping)).collect(Collectors.toSet());
        Optional<List<BulletError>> errors = TypeChecker.validateListSubTypes(node, argTypes);
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN, mapping);
        }
        return setType(node, TypeChecker.getListType(argTypes), mapping);
    }

    @Override
    protected Type visitNullPredicate(NullPredicateNode node, QuerySchema schema) {
        Type argType = process(node.getExpression(), mapping);
        Operation op = node.isNot() ? Operation.IS_NOT_NULL : Operation.IS_NULL;
        Optional<List<BulletError>> errors = TypeChecker.validateUnaryType(null, argType, op);
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
        }
        return setType(node, TypeChecker.getUnaryType(op), mapping);
    }

    @Override
    protected Type visitUnaryExpression(UnaryExpressionNode node, QuerySchema schema) {
        Type argType = process(node.getExpression(), mapping);
        Optional<List<BulletError>> errors = TypeChecker.validateUnaryType(node, argType, node.getOp());
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
        }
        return setType(node, TypeChecker.getUnaryType(node.getOp()), mapping);
    }

    @Override
    protected Type visitNAryExpression(NAryExpressionNode node, QuerySchema schema) {
        List<Type> argTypes = node.getExpressions().stream().map(processFunc(mapping)).collect(Collectors.toList());
        Optional<List<BulletError>> errors = TypeChecker.validateNAryType(node, argTypes, node.getOp());
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN, mapping);
        }
        return setType(node, TypeChecker.getNAryType(argTypes, node.getOp()), mapping);
    }

    @Override
    protected Type visitGroupOperation(GroupOperationNode node, QuerySchema schema) {
        if (node.getOp() == GroupOperation.GroupOperationType.COUNT) {
            return setType(node, Type.LONG, mapping);
        }
        Type argType = process(node.getExpression(), mapping);
        Optional<List<BulletError>> errors = TypeChecker.validateNumericType(node, argType);
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.DOUBLE, mapping);
        }
        return setType(node, TypeChecker.getAggregateType(argType, node.getOp()), mapping);
    }

    @Override
    protected Type visitCountDistinct(CountDistinctNode node, QuerySchema schema) {
        List<Type> argTypes = node.getExpressions().stream().map(processFunc(mapping)).collect(Collectors.toList());
        Optional<List<BulletError>> errors = TypeChecker.validatePrimitiveTypes(node, argTypes);
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
        }
        return setType(node, Type.LONG, mapping);
    }

    @Override
    protected Type visitDistribution(DistributionNode node, QuerySchema schema) {
        Type argType = process(node.getExpression(), mapping);
        TypeChecker.validateNumericType(node, argType).ifPresent(errors -> processedQuery.getErrors().addAll(errors));
        return Type.UNKNOWN;
    }

    @Override
    protected Type visitTopK(TopKNode node, QuerySchema schema) {
        List<Type> argTypes = node.getExpressions().stream().map(processFunc(mapping)).collect(Collectors.toList());
        TypeChecker.validatePrimitiveTypes(node, argTypes).ifPresent(errors -> processedQuery.getErrors().addAll(errors));
        return Type.UNKNOWN;
    }

    @Override
    protected Type visitCastExpression(CastExpressionNode node, QuerySchema schema) {
        Type argType = process(node.getExpression(), mapping);
        Optional<List<BulletError>> errors = TypeChecker.validateCastType(node, argType, node.getCastType());
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
        }
        return setType(node, node.getCastType(), mapping);
    }

    @Override
    protected Type visitBinaryExpression(BinaryExpressionNode node, QuerySchema schema) {
        Type leftType = process(node.getLeft(), mapping);
        Type rightType = process(node.getRight(), mapping);
        Optional<List<BulletError>> errors = TypeChecker.validateBinaryType(node, leftType, rightType, node.getOp());
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());




            // TODO
            // getBestBinaryType ..?

            if (node.getOp() == Operation.FILTER) {
                return setType(node, Type.UNKNOWN, mapping);
            }
            return setType(node, TypeChecker.getBinaryType(Type.DOUBLE, Type.DOUBLE, node.getOp()), mapping);
        }
        return setType(node, TypeChecker.getBinaryType(leftType, rightType, node.getOp()), mapping);
    }

    @Override
    protected Type visitParenthesesExpression(ParenthesesExpressionNode node, QuerySchema schema) {
        Type type = process(node.getExpression(), mapping);
        return setType(node, type, mapping);
    }

    @Override
    protected Type visitLiteral(LiteralNode node, QuerySchema schema) {
        // This shouldn't be called since literals have a type to begin with.
        throw new ParsingException("Literal missing its type.");
    }

    private Type setType(ExpressionNode node, Type type, QuerySchema schema) {
        // An expression is absent if it does not exist in the projection or computation tree.
        mapping.computeIfAbsent(node, k -> new TypeExpression()).setType(type);
        return type;
    }

    public static Type visit(Node node, QuerySchema schema) {
        return INSTANCE.process(node, schema);
    }

    public static void visit(Collection<? extends Node> nodes, QuerySchema schema) {
        nodes.forEach(INSTANCE.process(schema));
    }
}
