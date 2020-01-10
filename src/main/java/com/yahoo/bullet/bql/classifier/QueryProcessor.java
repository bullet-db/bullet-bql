package com.yahoo.bullet.bql.classifier;

import com.yahoo.bullet.bql.tree.CastExpressionNode;
import com.yahoo.bullet.bql.tree.CountDistinctNode;
import com.yahoo.bullet.bql.tree.DefaultTraversalVisitor;
import com.yahoo.bullet.bql.tree.DistributionNode;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.FieldExpressionNode;
import com.yahoo.bullet.bql.tree.GroupByNode;
import com.yahoo.bullet.bql.tree.GroupOperationNode;
import com.yahoo.bullet.bql.tree.IdentifierNode;
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
import com.yahoo.bullet.bql.tree.WindowIncludeNode;
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

import java.util.stream.Collectors;

public class QueryProcessor extends DefaultTraversalVisitor<ProcessedQuery, ProcessedQuery> {
    @Override
    public ProcessedQuery process(Node node) {
        return process(node, new ProcessedQuery());
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
        // TODO valueAggregateNodes vs nonValueAggregateNodes
        for (GroupOperationNode groupOperationNode : context.getGroupOpNodes()) {
            FieldExpression expression = (FieldExpression) context.getExpression(groupOperationNode);
            String alias = context.getAliases().get(groupOperationNode);
            if (alias != null) {
                expression.setField(alias);
            } else {
                expression.setField(groupOperationNode.toFormatlessString());
            }
        }
        for (CountDistinctNode countDistinctNode : context.getCountDistinctNodes()) {
            FieldExpression expression = (FieldExpression) context.getExpression(countDistinctNode);
            String alias = context.getAliases().get(countDistinctNode);
            if (alias != null) {
                expression.setField(alias);
            } else {
                expression.setField(countDistinctNode.toFormatlessString());
            }
        }
        return context.validate();
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
        String recordDuration = node.getRecordDuration();
        if (timeDuration != null) {
            context.setTimeDuration(timeDuration.equalsIgnoreCase("MAX") ? Long.MAX_VALUE : Long.parseLong(timeDuration));
        }
        if (recordDuration != null) {
            context.setRecordDuration(recordDuration.equalsIgnoreCase("MAX") ? Long.MAX_VALUE : Long.parseLong(recordDuration));
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
        context.setWindowed(true);
        context.setEmitEvery(node.getEmitEvery());
        context.setEmitType(node.getEmitType());
        return super.visitWindow(node, context);
    }

    @Override
    protected ProcessedQuery visitWindowInclude(WindowIncludeNode node, ProcessedQuery context) {
        context.setFirst(node.getNumber());
        context.setIncludeUnit(node.getUnit());
        return context;
    }

    @Override
    protected ProcessedQuery visitExpression(ExpressionNode node, ProcessedQuery context) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected ProcessedQuery visitFieldExpression(FieldExpressionNode node, ProcessedQuery context) {
        if (context.getExpressionNodes().containsKey(node)) {
            return context;
        }

        FieldExpression expression = new FieldExpression(node.getField().getValue(),
                                                         node.getIndex(),
                                                         node.getKey() != null ? node.getKey().getValue() : null,
                                                         node.getSubKey() != null ? node.getSubKey().getValue() : null,
                                                         node.getType(),
                                                         node.getPrimitiveType());

        context.getExpressionNodes().put(node, expression);

        return context;
    }

    @Override
    protected ProcessedQuery visitListExpression(ListExpressionNode node, ProcessedQuery context) {
        if (context.getExpressionNodes().containsKey(node)) {
            return context;
        }
        super.visitListExpression(node, context);

        ListExpression list = new ListExpression(node.getExpressions().stream().map(context::getExpression).collect(Collectors.toList()));

        context.getExpressionNodes().put(node, list);
        context.getSubExpressionNodes().addAll(node.getExpressions());

        if (node.getExpressions().stream().anyMatch(context::isAggregateOrSuperAggregate)) {
            context.getSuperAggregateNodes().add(node);
        }

        return context;
    }

    @Override
    protected ProcessedQuery visitNullPredicate(NullPredicateNode node, ProcessedQuery context) {
        if (context.getExpressionNodes().containsKey(node)) {
            return context;
        }
        super.visitNullPredicate(node, context);

        UnaryExpression expression = new UnaryExpression();
        expression.setOp(node.isNot() ? Operation.IS_NOT_NULL : Operation.IS_NULL);
        expression.setOperand(context.getExpression(node.getExpression()));

        context.getExpressionNodes().put(node, expression);
        context.getSubExpressionNodes().add(node.getExpression());

        if (context.isAggregateOrSuperAggregate(node.getExpression())) {
            context.getSuperAggregateNodes().add(node);
        }

        return context;
    }

    @Override
    protected ProcessedQuery visitUnaryExpression(UnaryExpressionNode node, ProcessedQuery context) {
        if (context.getExpressionNodes().containsKey(node)) {
            return context;
        }
        super.visitUnaryExpression(node, context);

        UnaryExpression expression = new UnaryExpression();
        expression.setOp(node.getOp());
        expression.setOperand(context.getExpression(node.getExpression()));

        context.getExpressionNodes().put(node, expression);
        context.getSubExpressionNodes().add(node.getExpression());

        if (context.isAggregateOrSuperAggregate(node.getExpression())) {
            context.getSuperAggregateNodes().add(node);
        }

        return context;
    }

    @Override
    protected ProcessedQuery visitNAryExpression(NAryExpressionNode node, ProcessedQuery context) {
        if (context.getExpressionNodes().containsKey(node)) {
            return context;
        }
        super.visitNAryExpression(node, context);

        NAryExpression expression = new NAryExpression();
        expression.setOp(node.getOp());
        expression.setOperands(node.getExpressions().stream().map(context::getExpression).collect(Collectors.toList()));

        context.getExpressionNodes().put(node, expression);
        context.getSubExpressionNodes().addAll(node.getExpressions());

        if (node.getExpressions().stream().anyMatch(context::isAggregateOrSuperAggregate)) {
            context.getSuperAggregateNodes().add(node);
        }

        return context;
    }

    @Override
    protected ProcessedQuery visitGroupOperation(GroupOperationNode node, ProcessedQuery context) {
        if (context.getExpressionNodes().containsKey(node)) {
            return context;
        }
        super.visitGroupOperation(node, context);

        FieldExpression expression = new FieldExpression();
        //expression.setField();

        context.getExpressionNodes().put(node, expression);
        context.getSubExpressionNodes().add(node.getExpression());

        if (context.isAggregateOrSuperAggregate(node.getExpression())) {
            context.getSuperAggregateNodes().add(node);
        }
        context.getAggregateNodes().add(node);
        context.getGroupOpNodes().add(node);
        context.getQueryTypeSet().add(ProcessedQuery.QueryType.GROUP);

        return context;
    }

    @Override
    protected ProcessedQuery visitCountDistinct(CountDistinctNode node, ProcessedQuery context) {
        if (context.getExpressionNodes().containsKey(node)) {
            return context;
        }
        super.visitCountDistinct(node, context);

        FieldExpression expression = new FieldExpression();
        //expression.setField();

        context.getExpressionNodes().put(node, expression);
        context.getSubExpressionNodes().addAll(node.getExpressions());

        if(node.getExpressions().stream().anyMatch(context::isAggregateOrSuperAggregate)) {
            context.getSuperAggregateNodes().add(node);
        }
        context.getAggregateNodes().add(node);
        context.getCountDistinctNodes().add(node);
        context.getQueryTypeSet().add(ProcessedQuery.QueryType.COUNT_DISTINCT);

        return context;
    }

    @Override
    protected ProcessedQuery visitDistribution(DistributionNode node, ProcessedQuery context) {
        if (context.getExpressionNodes().containsKey(node)) {
            return context;
        }
        super.visitDistribution(node, context);

        context.getSubExpressionNodes().add(node.getExpression());

        if (context.isAggregateOrSuperAggregate(node.getExpression())) {
            context.getSuperAggregateNodes().add(node);
        }
        context.getAggregateNodes().add(node);
        context.getDistributionNodes().add(node);
        context.getQueryTypeSet().add(ProcessedQuery.QueryType.DISTRIBUTION);

        return context;
    }

    @Override
    protected ProcessedQuery visitTopK(TopKNode node, ProcessedQuery context) {
        if (context.getExpressionNodes().containsKey(node)) {
            return context;
        }
        super.visitTopK(node, context);

        context.getSubExpressionNodes().addAll(node.getExpressions());

        if(node.getExpressions().stream().anyMatch(context::isAggregateOrSuperAggregate)) {
            context.getSuperAggregateNodes().add(node);
        }
        context.getAggregateNodes().add(node);
        context.getTopKNodes().add(node);
        context.getQueryTypeSet().add(ProcessedQuery.QueryType.TOP_K);

        return context;
    }

    @Override
    protected ProcessedQuery visitCastExpression(CastExpressionNode node, ProcessedQuery context) {
        if (context.getExpressionNodes().containsKey(node)) {
            return context;
        }
        super.visitCastExpression(node, context);

        CastExpression expression = new CastExpression(context.getExpression(node.getExpression()), node.getCastType());

        context.getExpressionNodes().put(node, expression);
        context.getSubExpressionNodes().add(node.getExpression());

        if (context.isAggregateOrSuperAggregate(node.getExpression())) {
            context.getSuperAggregateNodes().add(node);
        }

        return context;
    }

    @Override
    protected ProcessedQuery visitBinaryExpression(BinaryExpressionNode node, ProcessedQuery context) {
        if (context.getExpressionNodes().containsKey(node)) {
            return context;
        }
        super.visitBinaryExpression(node, context);

        BinaryExpression expression = new BinaryExpression(context.getExpression(node.getLeft()),
                                                           context.getExpression(node.getRight()),
                                                           node.getOp());

        context.getExpressionNodes().put(node, expression);
        context.getSubExpressionNodes().add(node.getLeft());
        context.getSubExpressionNodes().add(node.getRight());

        if (context.isAggregateOrSuperAggregate(node.getLeft()) || context.isAggregateOrSuperAggregate(node.getRight())) {
            context.getSuperAggregateNodes().add(node);
        }

        return context;
    }

    @Override
    protected ProcessedQuery visitParenthesesExpression(ParenthesesExpressionNode node, ProcessedQuery context) {
        if (context.getExpressionNodes().containsKey(node)) {
            return context;
        }
        super.visitParenthesesExpression(node, context);

        Expression expression = context.getExpression(node.getExpression());

        context.getExpressionNodes().put(node, expression);
        context.getSubExpressionNodes().add(node.getExpression());

        if (context.isAggregateOrSuperAggregate(node.getExpression())) {
            context.getSuperAggregateNodes().add(node);
        }

        return context;
    }

    @Override
    protected ProcessedQuery visitLiteral(LiteralNode node, ProcessedQuery context) {
        if (context.getExpressionNodes().containsKey(node)) {
            return context;
        }

        context.getExpressionNodes().put(node, new ValueExpression(node.getValue()));

        return context;
    }
}
