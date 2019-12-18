package com.yahoo.bullet.bql.classifier;

import com.yahoo.bullet.bql.tree.BooleanLiteralNode;
import com.yahoo.bullet.bql.tree.CastExpressionNode;
import com.yahoo.bullet.bql.tree.CountDistinctNode;
import com.yahoo.bullet.bql.tree.DecimalLiteralNode;
import com.yahoo.bullet.bql.tree.DefaultTraversalVisitor;
import com.yahoo.bullet.bql.tree.DistributionNode;
import com.yahoo.bullet.bql.tree.DoubleLiteralNode;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.GroupByNode;
import com.yahoo.bullet.bql.tree.GroupOperationNode;
import com.yahoo.bullet.bql.tree.IdentifierNode;
import com.yahoo.bullet.bql.tree.BinaryExpressionNode;
import com.yahoo.bullet.bql.tree.ListExpressionNode;
import com.yahoo.bullet.bql.tree.LiteralNode;
import com.yahoo.bullet.bql.tree.LongLiteralNode;
import com.yahoo.bullet.bql.tree.NAryExpressionNode;
import com.yahoo.bullet.bql.tree.Node;
import com.yahoo.bullet.bql.tree.NullLiteralNode;
import com.yahoo.bullet.bql.tree.NullPredicateNode;
import com.yahoo.bullet.bql.tree.OrderByNode;
import com.yahoo.bullet.bql.tree.QueryNode;
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.bql.tree.SelectNode;
import com.yahoo.bullet.bql.tree.SortItemNode;
import com.yahoo.bullet.bql.tree.StreamNode;
import com.yahoo.bullet.bql.tree.StringLiteralNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.bql.tree.UnaryExpressionNode;
import com.yahoo.bullet.bql.tree.WindowIncludeNode;
import com.yahoo.bullet.bql.tree.WindowNode;
import com.yahoo.bullet.parsing.expressions.BinaryExpression;
import com.yahoo.bullet.parsing.expressions.Expression;
import com.yahoo.bullet.parsing.expressions.FieldExpression;
import com.yahoo.bullet.parsing.expressions.ListExpression;
import com.yahoo.bullet.parsing.expressions.NAryExpression;
import com.yahoo.bullet.parsing.expressions.Operation;
import com.yahoo.bullet.parsing.expressions.UnaryExpression;
import com.yahoo.bullet.parsing.expressions.ValueExpression;
import com.yahoo.bullet.typesystem.Type;

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
            String alias = context.getAliases().get(groupOperationNode);
            if (alias != null) {
                FieldExpression expression = (FieldExpression) context.getExpression(groupOperationNode);
                expression.setField(alias);
            }
        }
        for (CountDistinctNode countDistinctNode : context.getCountDistinctNodes()) {
            String alias = context.getAliases().get(countDistinctNode);
            if (alias != null) {
                FieldExpression expression = (FieldExpression) context.getExpression(countDistinctNode);
                expression.setField(alias);
            }
        }
        return context.validate();
    }

    @Override
    protected ProcessedQuery visitSelect(SelectNode node, ProcessedQuery context) {
        context.getQueryTypeSet().add(ProcessedQuery.QueryType.SELECT_DISTINCT);
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
    protected ProcessedQuery visitListExpression(ListExpressionNode node, ProcessedQuery context) {
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
        super.visitDistribution(node, context);
/*
        node instanceof LinearDistributionNode;
        node instanceof RegionDistributionNode;
        node instanceof ManualDistributionNode;

        node.getAttributes();
        node.getExpression();
        node.getType();
*/
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
        super.visitTopK(node, context);
/*
        node.getExpressions();
        node.getSize();
        node.getThreshold();
*/
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
        super.visitCastExpression(node, context);

        Expression expression = context.getExpression(node.getExpression());
        expression.setType(node.getCastType());

        context.getExpressionNodes().put(node, expression);
        context.getSubExpressionNodes().add(node.getExpression());

        if (context.isAggregateOrSuperAggregate(node.getExpression())) {
            context.getSuperAggregateNodes().add(node);
        }

        return context;
    }

    @Override
    protected ProcessedQuery visitBinaryExpression(BinaryExpressionNode node, ProcessedQuery context) {
        super.visitBinaryExpression(node, context);

        BinaryExpression expression = new BinaryExpression();
        expression.setLeft(context.getExpression(node.getLeft()));
        expression.setRight(context.getExpression(node.getRight()));
        expression.setOp(node.getOp());

        context.getExpressionNodes().put(node, expression);
        context.getSubExpressionNodes().add(node.getLeft());
        context.getSubExpressionNodes().add(node.getRight());

        if (context.isAggregateOrSuperAggregate(node.getLeft()) || context.isAggregateOrSuperAggregate(node.getRight())) {
            context.getSuperAggregateNodes().add(node);
        }

        return context;
    }

    @Override
    protected ProcessedQuery visitIdentifier(IdentifierNode node, ProcessedQuery context) {
        FieldExpression expression = new FieldExpression();
        expression.setField(node.getValue());

        context.getExpressionNodes().put(node, expression);

        //node.getValue();
        //node.isDelimited();

        return context;
    }

    @Override
    protected ProcessedQuery visitLiteral(LiteralNode node, ProcessedQuery context) {
        throw new RuntimeException("shouldn't be called");
    }

    @Override
    protected ProcessedQuery visitNullLiteral(NullLiteralNode node, ProcessedQuery context) {
        ValueExpression expression = new ValueExpression();
        expression.setType(Type.NULL);

        context.getExpressionNodes().put(node, expression);

        return context;
    }

    @Override
    protected ProcessedQuery visitStringLiteral(StringLiteralNode node, ProcessedQuery context) {
        ValueExpression expression = new ValueExpression();
        expression.setValue(node.getValue());
        expression.setType(Type.STRING);

        context.getExpressionNodes().put(node, expression);

        return context;
    }

    @Override
    protected ProcessedQuery visitLongLiteral(LongLiteralNode node, ProcessedQuery context) {
        ValueExpression expression = new ValueExpression();
        expression.setValue(node.getValue().toString());
        expression.setType(Type.LONG);

        context.getExpressionNodes().put(node, expression);

        return context;
    }

    @Override
    protected ProcessedQuery visitDoubleLiteral(DoubleLiteralNode node, ProcessedQuery context) {
        ValueExpression expression = new ValueExpression();
        expression.setValue(node.getValue().toString());
        expression.setType(Type.DOUBLE);

        context.getExpressionNodes().put(node, expression);

        return context;
    }

    @Override
    protected ProcessedQuery visitDecimalLiteral(DecimalLiteralNode node, ProcessedQuery context) {
        ValueExpression expression = new ValueExpression();
        expression.setValue(node.getValue());
        //expression.setType(Type.FLOAT);
        expression.setType(Type.DOUBLE);

        context.getExpressionNodes().put(node, expression);

        return context;
    }

    @Override
    protected ProcessedQuery visitBooleanLiteral(BooleanLiteralNode node, ProcessedQuery context) {
        ValueExpression expression = new ValueExpression();
        expression.setValue(node.getValue().toString());
        expression.setType(Type.BOOLEAN);

        context.getExpressionNodes().put(node, expression);

        return context;
    }
}
