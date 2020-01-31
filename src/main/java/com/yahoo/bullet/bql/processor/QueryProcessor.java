/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.processor;

import com.yahoo.bullet.bql.tree.CastExpressionNode;
import com.yahoo.bullet.bql.tree.CountDistinctNode;
import com.yahoo.bullet.bql.tree.DefaultTraversalVisitor;
import com.yahoo.bullet.bql.tree.DistributionNode;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.FieldExpressionNode;
import com.yahoo.bullet.bql.tree.GroupByNode;
import com.yahoo.bullet.bql.tree.GroupOperationNode;
import com.yahoo.bullet.bql.tree.BinaryExpressionNode;
import com.yahoo.bullet.bql.tree.ListExpressionNode;
import com.yahoo.bullet.bql.tree.LiteralNode;
import com.yahoo.bullet.bql.tree.NAryExpressionNode;
import com.yahoo.bullet.bql.tree.Node;
import com.yahoo.bullet.bql.tree.NullPredicateNode;
import com.yahoo.bullet.bql.tree.OrderByNode;
import com.yahoo.bullet.bql.tree.ParenthesesExpressionNode;
import com.yahoo.bullet.bql.tree.QueryNode;
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.bql.tree.SelectNode;
import com.yahoo.bullet.bql.tree.SortItemNode;
import com.yahoo.bullet.bql.tree.StreamNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.bql.tree.UnaryExpressionNode;
import com.yahoo.bullet.bql.tree.WindowNode;
import com.yahoo.bullet.parsing.expressions.BinaryExpression;
import com.yahoo.bullet.parsing.expressions.CastExpression;
import com.yahoo.bullet.parsing.expressions.Expression;
import com.yahoo.bullet.parsing.expressions.FieldExpression;
import com.yahoo.bullet.parsing.expressions.ListExpression;
import com.yahoo.bullet.parsing.expressions.NAryExpression;
import com.yahoo.bullet.parsing.expressions.Operation;
import com.yahoo.bullet.parsing.expressions.UnaryExpression;
import com.yahoo.bullet.parsing.expressions.ValueExpression;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class QueryProcessor extends DefaultTraversalVisitor<ProcessedQuery, ProcessedQuery> {
    @Override
    public ProcessedQuery process(Node node) {
        return process(node, new ProcessedQuery());
    }

    @Override
    public ProcessedQuery process(Node node, ProcessedQuery context) {
        if (node instanceof ExpressionNode && context.getExpressionNodes().containsKey(node)) {
            return context;
        }
        return super.process(node, context);
    }

    @Override
    protected ProcessedQuery visitNode(Node node, ProcessedQuery context) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected ProcessedQuery visitQuery(QueryNode node, ProcessedQuery context) {
        super.visitQuery(node, context);
        context.setWhereNode(node.getWhere());
        context.setHavingNode(node.getHaving());
        if (node.getLimit() != null) {
            context.setLimit(Integer.parseInt(node.getLimit()));
        }
        if (context.getQueryTypeSet().isEmpty()) {
            context.getQueryTypeSet().add(ProcessedQuery.QueryType.SELECT);
        }
        // Set field expressions for group ops and count distinct to point to the correct field names
        for (GroupOperationNode groupOperationNode : context.getGroupOpNodes()) {
            FieldExpression expression = (FieldExpression) context.getExpression(groupOperationNode);
            String alias = context.getAliases().get(groupOperationNode);
            if (alias != null) {
                expression.setField(alias);
            } else {
                expression.setField(groupOperationNode.getName());
            }
        }
        for (CountDistinctNode countDistinctNode : context.getCountDistinctNodes()) {
            FieldExpression expression = (FieldExpression) context.getExpression(countDistinctNode);
            String alias = context.getAliases().get(countDistinctNode);
            if (alias != null) {
                expression.setField(alias);
            } else {
                expression.setField(countDistinctNode.getName());
            }
        }
        context.validate();
        return context;
    }

    @Override
    protected ProcessedQuery visitSelect(SelectNode node, ProcessedQuery context) {
        if (node.isDistinct()) {
            context.getQueryTypeSet().add(ProcessedQuery.QueryType.SELECT_DISTINCT);
        }
        return super.visitSelect(node, context);
    }

    @Override
    protected ProcessedQuery visitSelectItem(SelectItemNode node, ProcessedQuery context) {
        super.visitSelectItem(node, context);
        if (node.getExpression() != null) {
            context.getSelectNodes().add(node);
            if (node.getAlias() != null) {
                context.getAliases().put(node.getExpression(), node.getAlias().getValue());
            }
        } else {
            context.getQueryTypeSet().add(ProcessedQuery.QueryType.SELECT_ALL);
        }
        return context;
    }

    @Override
    protected ProcessedQuery visitStream(StreamNode node, ProcessedQuery context) {
        String timeDuration = node.getTimeDuration();
        if (timeDuration != null) {
            context.setTimeDuration(timeDuration.equalsIgnoreCase("MAX") ? Long.MAX_VALUE : Long.parseLong(timeDuration));
        }
        return context;
    }

    @Override
    protected ProcessedQuery visitGroupBy(GroupByNode node, ProcessedQuery context) {
        super.visitGroupBy(node, context);
        context.getGroupByNodes().addAll(node.getExpressions());
        context.getQueryTypeSet().add(ProcessedQuery.QueryType.GROUP);
        return context;
    }

    @Override
    protected ProcessedQuery visitOrderBy(OrderByNode node, ProcessedQuery context) {
        return super.visitOrderBy(node, context);
    }

    @Override
    protected ProcessedQuery visitSortItem(SortItemNode node, ProcessedQuery context) {
        super.visitSortItem(node, context);
        context.getOrderByNodes().add(node);
        return context;
    }

    @Override
    protected ProcessedQuery visitWindow(WindowNode node, ProcessedQuery context) {
        context.setWindow(node);
        return context;
    }

    @Override
    protected ProcessedQuery visitExpression(ExpressionNode node, ProcessedQuery context) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected ProcessedQuery visitFieldExpression(FieldExpressionNode node, ProcessedQuery context) {
        FieldExpression expression = new FieldExpression(node.getField().getValue(),
                                                         node.getIndex(),
                                                         node.getKey() != null ? node.getKey().getValue() : null,
                                                         node.getSubKey() != null ? node.getSubKey().getValue() : null,
                                                         node.getType(),
                                                         node.getPrimitiveType());
        context.addExpression(node, expression);
        return context;
    }

    @Override
    protected ProcessedQuery visitListExpression(ListExpressionNode node, ProcessedQuery context) {
        super.visitListExpression(node, context);

        ListExpression listExpression = new ListExpression(node.getExpressions().stream().map(context::getExpression).collect(Collectors.toList()));

        context.addExpression(node, listExpression, node.getExpressions());

        return context;
    }

    @Override
    protected ProcessedQuery visitNullPredicate(NullPredicateNode node, ProcessedQuery context) {
        super.visitNullPredicate(node, context);

        Operation op = node.isNot() ? Operation.IS_NOT_NULL : Operation.IS_NULL;

        UnaryExpression expression = new UnaryExpression(context.getExpression(node.getExpression()), op);

        context.addExpression(node, expression, node.getExpression());

        return context;
    }

    @Override
    protected ProcessedQuery visitUnaryExpression(UnaryExpressionNode node, ProcessedQuery context) {
        super.visitUnaryExpression(node, context);

        UnaryExpression expression = new UnaryExpression(context.getExpression(node.getExpression()), node.getOp());

        context.addExpression(node, expression, node.getExpression());

        return context;
    }

    @Override
    protected ProcessedQuery visitNAryExpression(NAryExpressionNode node, ProcessedQuery context) {
        super.visitNAryExpression(node, context);

        List<Expression> operands = node.getExpressions().stream().map(context::getExpression).collect(Collectors.toList());

        NAryExpression expression = new NAryExpression(operands, node.getOp());

        context.addExpression(node, expression, node.getExpressions());

        return context;
    }

    @Override
    protected ProcessedQuery visitGroupOperation(GroupOperationNode node, ProcessedQuery context) {
        super.visitGroupOperation(node, context);

        context.addExpression(node, new FieldExpression(), node.getExpression());

        context.getAggregateNodes().add(node);
        context.getGroupOpNodes().add(node);
        context.getQueryTypeSet().add(ProcessedQuery.QueryType.GROUP);

        return context;
    }

    @Override
    protected ProcessedQuery visitCountDistinct(CountDistinctNode node, ProcessedQuery context) {
        super.visitCountDistinct(node, context);

        context.addExpression(node, new FieldExpression(), node.getExpressions());

        context.getAggregateNodes().add(node);
        context.getCountDistinctNodes().add(node);
        context.getQueryTypeSet().add(ProcessedQuery.QueryType.COUNT_DISTINCT);

        return context;
    }

    @Override
    protected ProcessedQuery visitDistribution(DistributionNode node, ProcessedQuery context) {
        super.visitDistribution(node, context);

        context.addExpression(node, null, node.getExpression());

        context.getAggregateNodes().add(node);
        context.getDistributionNodes().add(node);
        context.getQueryTypeSet().add(ProcessedQuery.QueryType.DISTRIBUTION);

        return context;
    }

    @Override
    protected ProcessedQuery visitTopK(TopKNode node, ProcessedQuery context) {
        super.visitTopK(node, context);

        context.addExpression(node, null, node.getExpressions());

        context.getAggregateNodes().add(node);
        context.getTopKNodes().add(node);
        context.getQueryTypeSet().add(ProcessedQuery.QueryType.TOP_K);

        return context;
    }

    @Override
    protected ProcessedQuery visitCastExpression(CastExpressionNode node, ProcessedQuery context) {
        super.visitCastExpression(node, context);

        CastExpression expression = new CastExpression(context.getExpression(node.getExpression()), node.getCastType());

        context.addExpression(node, expression, node.getExpression());

        return context;
    }

    @Override
    protected ProcessedQuery visitBinaryExpression(BinaryExpressionNode node, ProcessedQuery context) {
        super.visitBinaryExpression(node, context);

        BinaryExpression expression = new BinaryExpression(context.getExpression(node.getLeft()),
                                                           context.getExpression(node.getRight()),
                                                           node.getOp());

        context.addExpression(node, expression, Arrays.asList(node.getLeft(), node.getRight()));

        return context;
    }

    @Override
    protected ProcessedQuery visitParenthesesExpression(ParenthesesExpressionNode node, ProcessedQuery context) {
        super.visitParenthesesExpression(node, context);

        Expression expression = context.getExpression(node.getExpression());

        context.addExpression(node, expression, node.getExpression());

        return context;
    }

    @Override
    protected ProcessedQuery visitLiteral(LiteralNode node, ProcessedQuery context) {
        context.addExpression(node, new ValueExpression(node.getValue()));

        return context;
    }
}
