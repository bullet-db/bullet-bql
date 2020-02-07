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
import com.yahoo.bullet.bql.tree.ListExpressionNode;
import com.yahoo.bullet.bql.tree.LiteralNode;
import com.yahoo.bullet.bql.tree.NAryExpressionNode;
import com.yahoo.bullet.bql.tree.Node;
import com.yahoo.bullet.bql.tree.NullPredicateNode;
import com.yahoo.bullet.bql.tree.ParenthesesExpressionNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.bql.tree.UnaryExpressionNode;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.parsing.expressions.Operation;
import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Sets and returns type for visited expression. Accumulates errors found in processed query.
 *
 */
@Getter
public class ExpressionValidator extends DefaultTraversalVisitor<Type, Map<String, Type>> {
    private ProcessedQuery processedQuery;

    @Override
    public Type process(Node node) {
        return process(node, new HashMap<>());
    }

    @Override
    public Type process(Node node, Map<String, Type> context) {
        Type type = processedQuery.getExpression((ExpressionNode) node).getType();
        if (type != null) {
            return type;
        }
        return super.process(node, context);
    }

    @Override
    protected Type visitFieldExpression(FieldExpressionNode node, Map<String, Type> context) {
        Type fieldType = context.get(node.getField().getValue());
        Optional<List<BulletError>> errors = TypeChecker.validateFieldType(fieldType, node.hasIndexOrKey(), node.hasSubKey());
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN);
        }
        return setType(node, TypeChecker.getFieldType(fieldType, node.hasIndexOrKey(), node.hasSubKey()));
    }

    @Override
    protected Type visitListExpression(ListExpressionNode node, Map<String, Type> context) {
        List<Type> argTypes = node.getExpressions().stream().map(processFunc(context)).collect(Collectors.toList());
        Optional<List<BulletError>> errors = TypeChecker.validateListTypes(argTypes);
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN);
        }
        return setType(node, TypeChecker.getListType(argTypes));
    }

    @Override
    protected Type visitNullPredicate(NullPredicateNode node, Map<String, Type> context) {
        Type argType = process(node.getExpression(), context);
        Operation op = node.isNot() ? Operation.IS_NOT_NULL : Operation.IS_NULL;
        Optional<List<BulletError>> errors = TypeChecker.validateUnaryType(argType, op);
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN);
        }
        return setType(node, TypeChecker.getUnaryType(op));
    }

    @Override
    protected Type visitUnaryExpression(UnaryExpressionNode node, Map<String, Type> context) {
        Type argType = process(node.getExpression(), context);
        Optional<List<BulletError>> errors = TypeChecker.validateUnaryType(argType, node.getOp());
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN);
        }
        return setType(node, TypeChecker.getUnaryType(node.getOp()));
    }

    @Override
    protected Type visitNAryExpression(NAryExpressionNode node, Map<String, Type> context) {
        List<Type> argTypes = node.getExpressions().stream().map(processFunc(context)).collect(Collectors.toList());
        Optional<List<BulletError>> errors = TypeChecker.validateNAryType(argTypes, node.getOp());
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN);
        }
        return setType(node, TypeChecker.getNAryType(argTypes, node.getOp()));
    }

    @Override
    protected Type visitGroupOperation(GroupOperationNode node, Map<String, Type> context) {
        if (node.getOp() == GroupOperation.GroupOperationType.COUNT) {
            return Type.LONG;
        }
        Type argType = process(node.getExpression(), context);
        Optional<List<BulletError>> errors = TypeChecker.validateAggregateType(argType);
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN);
        }
        return setType(node, TypeChecker.getAggregateType(argType, node.getOp()));
    }

    @Override
    protected Type visitCountDistinct(CountDistinctNode node, Map<String, Type> context) {
        List<Type> argTypes = node.getExpressions().stream().map(processFunc(context)).collect(Collectors.toList());
        Optional<List<BulletError>> errors = TypeChecker.validateCountDistinctType(argTypes);
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN);
        }
        return setType(node, Type.LONG);
    }

    @Override
    protected Type visitDistribution(DistributionNode node, Map<String, Type> context) {
        Type argType = process(node.getExpression(), context);
        TypeChecker.validateDistributionType(argType).ifPresent(errors -> processedQuery.getErrors().addAll(errors));
        return setType(node, Type.UNKNOWN);
    }

    @Override
    protected Type visitTopK(TopKNode node, Map<String, Type> context) {
        List<Type> argTypes = node.getExpressions().stream().map(processFunc(context)).collect(Collectors.toList());
        TypeChecker.validateTopKType(argTypes).ifPresent(errors -> processedQuery.getErrors().addAll(errors));
        return setType(node, Type.UNKNOWN);
    }

    @Override
    protected Type visitCastExpression(CastExpressionNode node, Map<String, Type> context) {
        Type argType = process(node.getExpression(), context);
        Optional<List<BulletError>> errors = TypeChecker.validateCastType(argType, node.getCastType());
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN);
        }
        return setType(node, node.getCastType());
    }

    @Override
    protected Type visitBinaryExpression(BinaryExpressionNode node, Map<String, Type> context) {
        Type leftType = process(node.getLeft(), context);
        Type rightType = process(node.getRight(), context);
        Optional<List<BulletError>> errors = TypeChecker.validateBinaryType(leftType, rightType, node.getOp());
        if (errors.isPresent()) {
            processedQuery.getErrors().addAll(errors.get());
            return setType(node, Type.UNKNOWN);
        }
        return setType(node, TypeChecker.getBinaryType(leftType, rightType, node.getOp()));
    }

    @Override
    protected Type visitParenthesesExpression(ParenthesesExpressionNode node, Map<String, Type> context) {
        Type type = process(node.getExpression(), context);
        return setType(node, type);
    }

    @Override
    protected Type visitLiteral(LiteralNode node, Map<String, Type> context) {
        // This shouldn't be called since this node/expression should already have a type.
        throw new ParsingException("how did u get here");
    }

    private Type setType(ExpressionNode node, Type type) {
        processedQuery.getExpression(node).setType(type);
        return type;
    }
}
