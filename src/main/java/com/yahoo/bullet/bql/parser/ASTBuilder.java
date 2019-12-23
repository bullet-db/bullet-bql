/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/parser/AstBuilder.java
 */
package com.yahoo.bullet.bql.parser;

import com.yahoo.bullet.aggregations.Distribution.Type;
import com.yahoo.bullet.bql.tree.CastExpressionNode;
import com.yahoo.bullet.bql.tree.CountDistinctNode;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.GroupByNode;
import com.yahoo.bullet.bql.tree.GroupOperationNode;
import com.yahoo.bullet.bql.tree.IdentifierNode;
import com.yahoo.bullet.bql.tree.BinaryExpressionNode;
import com.yahoo.bullet.bql.tree.LinearDistributionNode;
import com.yahoo.bullet.bql.tree.ListExpressionNode;
import com.yahoo.bullet.bql.tree.LiteralNode;
import com.yahoo.bullet.bql.tree.ManualDistributionNode;
import com.yahoo.bullet.bql.tree.NAryExpressionNode;
import com.yahoo.bullet.bql.tree.Node;
import com.yahoo.bullet.bql.tree.NullPredicateNode;
import com.yahoo.bullet.bql.tree.OrderByNode;
import com.yahoo.bullet.bql.tree.ParenthesesExpressionNode;
import com.yahoo.bullet.bql.tree.QueryNode;
import com.yahoo.bullet.bql.tree.RegionDistributionNode;
import com.yahoo.bullet.bql.tree.SelectNode;
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.bql.tree.SortItemNode;
import com.yahoo.bullet.bql.tree.StreamNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.bql.tree.UnaryExpressionNode;
import com.yahoo.bullet.bql.tree.WindowIncludeNode;
import com.yahoo.bullet.bql.tree.WindowNode;
import com.yahoo.bullet.parsing.Window.Unit;
import com.yahoo.bullet.parsing.expressions.Operation;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.List;
import java.util.stream.Collectors;

import static com.yahoo.bullet.aggregations.Distribution.Type.CDF;
import static com.yahoo.bullet.aggregations.Distribution.Type.PMF;
import static com.yahoo.bullet.aggregations.Distribution.Type.QUANTILE;

class ASTBuilder extends BQLBaseBaseVisitor<Node> {
    @Override
    public Node visitQuery(BQLBaseParser.QueryContext context) {
        return new QueryNode((SelectNode) visit(context.select()),
                             (StreamNode) visit(context.stream()),
                             (ExpressionNode) visitIfPresent(context.where),
                             (GroupByNode) visitIfPresent(context.groupBy()),
                             (ExpressionNode) visitIfPresent(context.having),
                             (OrderByNode) visitIfPresent(context.orderBy()),
                             (WindowNode) visitIfPresent(context.window()),
                             getTextIfPresent(context.limit));
    }

    @Override
    public Node visitSelect(BQLBaseParser.SelectContext context) {
        return new SelectNode(context.DISTINCT() != null,
                              visit(context.selectItem(), SelectItemNode.class));
    }

    @Override
    public Node visitSelectItem(BQLBaseParser.SelectItemContext context) {
        return new SelectItemNode(context.ASTERISK() != null,
                                  (ExpressionNode) visitIfPresent(context.expression()),
                                  (IdentifierNode) visitIfPresent(context.identifier()));
    }

    @Override
    public Node visitStream(BQLBaseParser.StreamContext context) {
        return new StreamNode(getTextIfPresent(context.timeDuration),
                              getTextIfPresent(context.recordDuration));
    }

    @Override
    public Node visitGroupBy(BQLBaseParser.GroupByContext context) {
        return new GroupByNode(visit(context.expression(), ExpressionNode.class));
    }


    @Override
    public Node visitOrderBy(BQLBaseParser.OrderByContext context) {
        return new OrderByNode(visit(context.sortItem(), SortItemNode.class));
    }

    @Override
    public Node visitSortItem(BQLBaseParser.SortItemContext context) {
        return new SortItemNode((ExpressionNode) visit(context.expression()),
                                getOrdering(context.ordering));
    }

    @Override
    public Node visitWindow(BQLBaseParser.WindowContext context) {
        Long emitEvery = Long.parseLong(context.emitEvery.getText());
        Unit emitType = Unit.valueOf(context.emitType.getText().toUpperCase());
        return new WindowNode(emitEvery,
                              emitType,
                              (WindowIncludeNode) visitIfPresent(context.include()));
    }

    @Override
    public Node visitInclude(BQLBaseParser.IncludeContext context) {
        //Optional<IncludeType> type = getTextIfPresent(context.includeType).map(String::toUpperCase)
        //                                                                  .map(IncludeType::valueOf);
        //Optional<Long> number = getTextIfPresent(context.INTEGER_VALUE()).map(Long::parseLong);
        //Unit unit = Unit.valueOf(context.includeUnit.getText().toUpperCase());
        //return new WindowIncludeNode(type, number, unit);
        return new WindowIncludeNode(getTextIfPresent(context.INTEGER_VALUE()),
                                     context.includeUnit.getText());
    }
/*
    @Override
    public Node visitValue(BQLBaseParser.ValueContext context) {
        return visit(context.valueExpression());
    }

    @Override
    public Node visitField(BQLBaseParser.FieldContext context) {
        return visit(context.identifier());
    }

    @Override
    public Node visitList(BQLBaseParser.ListContext context) {
        return visit(context.listExpression());
    }
*/
    @Override
    public Node visitListExpression(BQLBaseParser.ListExpressionContext context) {
        return new ListExpressionNode(visit(context.expression(), ExpressionNode.class));
    }

    @Override
    public Node visitNullPredicate(BQLBaseParser.NullPredicateContext context) {
        return new NullPredicateNode((ExpressionNode) visit(context.expression()),
                                     context.NOT() != null);
    }
/*
    @Override
    public Node visitUnary(BQLBaseParser.UnaryContext context) {
        return visit(context.unaryExpression());
    }
*/
    @Override
    public Node visitUnaryExpression(BQLBaseParser.UnaryExpressionContext context) {
        return new UnaryExpressionNode(getOperation(context.op),
                                       (ExpressionNode) visit(context.expression()));
    }
/*
    @Override
    public Node visitFunction(BQLBaseParser.FunctionContext context) {
        return visit(context.functionExpression());
    }
*/

    @Override
    public Node visitBinary(BQLBaseParser.BinaryContext context) {
        return new BinaryExpressionNode((ExpressionNode) visit(context.left),
                                       (ExpressionNode) visit(context.right),
                                       getOperation(context.binaryFunction().op));
    }

    @Override
    public Node visitNAry(BQLBaseParser.NAryContext context) {
        return new NAryExpressionNode(getOperation(context.op),
                                      visit(context.expression(), ExpressionNode.class));
    }
/*
    @Override
    public Node visitAggregate(BQLBaseParser.AggregateContext context) {
        return visit(context.aggregateExpression());
    }
*/
    @Override
    public Node visitGroupOperation(BQLBaseParser.GroupOperationContext context) {
        return new GroupOperationNode(context.op.getText(),
                                      (ExpressionNode) visitIfPresent(context.expression()));
    }

    @Override
    public Node visitCountDistinct(BQLBaseParser.CountDistinctContext context) {
        return new CountDistinctNode(visit(context.expression(), ExpressionNode.class));
    }

    @Override
    public Node visitDistribution(BQLBaseParser.DistributionContext context) {
        Type type = getDistributionType(context);
        ExpressionNode expression = (ExpressionNode) visit(context.expression());
        switch(context.inputMode().iMode.getType()) {
            case BQLBaseLexer.LINEAR: {
                Long numberOfPoints = Long.parseLong(context.inputMode().numberOfPoints.getText());
                return new LinearDistributionNode(type, expression, numberOfPoints);
            }
            case BQLBaseLexer.REGION: {
                Double start = getSignedNumber(context.inputMode().start).doubleValue();
                Double end = getSignedNumber(context.inputMode().end).doubleValue();
                Double increment = getSignedNumber(context.inputMode().increment).doubleValue();
                return new RegionDistributionNode(type, expression, start, end, increment);
            }
            case BQLBaseLexer.MANUAL: {
                List<Double> points = context.inputMode().number().stream().map(this::getSignedNumber).map(Number::doubleValue).collect(Collectors.toList());
                return new ManualDistributionNode(type, expression, points);
            }
        }
        throw new ParsingException("Unknown input mode");
    }

    @Override
    public Node visitTopK(BQLBaseParser.TopKContext context) {
        return visit(context.topKConfig());
    }

    @Override
    public Node visitTopKConfig(BQLBaseParser.TopKConfigContext context) {
        return new TopKNode(context.size.getText(),
                            getTextIfPresent(context.threshold),
                            visit(context.expression(), ExpressionNode.class));
    }

    @Override
    public Node visitCast(BQLBaseParser.CastContext context) {
        return new CastExpressionNode((ExpressionNode) visit(context.expression()),
                                      context.castType().getText());
    }

    @Override
    public Node visitInfix(BQLBaseParser.InfixContext context) {
        return new BinaryExpressionNode((ExpressionNode) visit(context.left),
                                       (ExpressionNode) visit(context.right),
                                       getOperation(context.op));
    }

    @Override
    public Node visitParentheses(BQLBaseParser.ParenthesesContext context) {
        return new ParenthesesExpressionNode((ExpressionNode) visit(context.expression()));
    }


    @Override
    public Node visitUnquotedIdentifier(BQLBaseParser.UnquotedIdentifierContext context) {
        return new IdentifierNode(context.getText());
    }

    @Override
    public Node visitQuotedIdentifier(BQLBaseParser.QuotedIdentifierContext context) {
        return new IdentifierNode(unquoteDouble(context.getText()));
    }

    // ************** Literals **************

    @Override
    public Node visitNullLiteral(BQLBaseParser.NullLiteralContext context) {
        return new LiteralNode(null);
    }

    @Override
    public Node visitNumericLiteral(BQLBaseParser.NumericLiteralContext context) {
        return new LiteralNode(getSignedNumber(context.number()));
    }

    @Override
    public Node visitBooleanValue(BQLBaseParser.BooleanValueContext context) {
        return new LiteralNode(Boolean.valueOf(context.getText()));
    }

    @Override
    public Node visitStringLiteral(BQLBaseParser.StringLiteralContext context) {
        return new LiteralNode(unquoteSingle(context.getText()));
    }

    // ***************** Helpers *****************

    @Override
    protected Node defaultResult() {
        return null;
    }

    @Override
    protected Node aggregateResult(Node aggregate, Node nextResult) throws UnsupportedOperationException {
        if (aggregate != null || nextResult == null) {
            throw new UnsupportedOperationException("Not implemented");
        }
        return nextResult;
    }

    private static SortItemNode.Ordering getOrdering(Token token) {
        return token != null && token.getType() == BQLBaseLexer.DESC ? SortItemNode.Ordering.DESCENDING :
                                                                       SortItemNode.Ordering.ASCENDING;
    }

    private Node visitIfPresent(ParserRuleContext context) {
        return context != null ? visit(context) : null;
    }

    private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> type) {
        return contexts.stream().map(this::visit)
                                .map(type::cast)
                                .collect(Collectors.toList());
    }

    private static String unquoteSingle(String value) {
        return value.substring(1, value.length() - 1).replace("''", "'");
    }

    private static String unquoteDouble(String value) {
        return value.substring(1, value.length() - 1).replace("\"\"", "\"");
    }

    private static String getTextIfPresent(Token token) {
        return token != null ? token.getText() : null;
    }

    private static String getTextIfPresent(TerminalNode node) {
        return node != null ? node.getText() : null;
    }

    private static Operation getOperation(Token token) {
        switch (token.getType()) {
            case BQLBaseLexer.PLUS:
                return Operation.ADD;
            case BQLBaseLexer.MINUS:
                return Operation.SUB;
            case BQLBaseLexer.ASTERISK:
                return Operation.MUL;
            case BQLBaseLexer.SLASH:
                return Operation.DIV;
            case BQLBaseLexer.EQ:
                return Operation.EQUALS;
            case BQLBaseLexer.NEQ:
                return Operation.NOT_EQUALS;
            case BQLBaseLexer.GT:
                return Operation.GREATER_THAN;
            case BQLBaseLexer.LT:
                return Operation.LESS_THAN;
            case BQLBaseLexer.GTE:
                return Operation.GREATER_THAN_OR_EQUALS;
            case BQLBaseLexer.LTE:
                return Operation.LESS_THAN_OR_EQUALS;
            case BQLBaseLexer.RLIKE:
                return Operation.REGEX_LIKE;
            case BQLBaseLexer.SIZEIS:
                return Operation.SIZE_IS;
            case BQLBaseLexer.CONTAINSKEY:
                return Operation.CONTAINS_KEY;
            case BQLBaseLexer.CONTAINSVALUE:
                return Operation.CONTAINS_VALUE;
            case BQLBaseLexer.AND:
                return Operation.AND;
            case BQLBaseLexer.OR:
                return Operation.OR;
            case BQLBaseLexer.XOR:
                return Operation.XOR;
            case BQLBaseLexer.FILTER:
                return Operation.FILTER;
            case BQLBaseLexer.NOT:
                return Operation.NOT;
            case BQLBaseLexer.SIZEOF:
                return Operation.SIZE_OF;
            case BQLBaseLexer.IF:
                return Operation.IF;
        }
        throw new ParsingException("Unknown operation");
    }

    private Type getDistributionType(BQLBaseParser.DistributionContext context) {
        switch (context.distributionType().getText().toUpperCase()) {
            case "QUANTILE":
                return QUANTILE;
            case "FREQ":
                return PMF;
            case "CUMFREQ":
                return CDF;
        }
        throw new ParsingException("Unknown distribution");
    }

    private Number getSignedNumber(BQLBaseParser.NumberContext context) {
        boolean negative = context.MINUS() != null;
        String value = context.value.getText();
        switch (context.value.getType()) {
            case BQLBaseLexer.INTEGER_VALUE:
                return negative ? -Integer.valueOf(value) : Integer.valueOf(value);
            case BQLBaseLexer.LONG_VALUE:
                value = value.substring(0, value.length() - 1);
                return negative ? -Long.valueOf(value) : Long.valueOf(value);
            case BQLBaseLexer.FLOAT_VALUE:
                return negative ? -Float.valueOf(value) : Float.valueOf(value);
            case BQLBaseLexer.DOUBLE_VALUE:
                return negative ? -Double.valueOf(value) : Double.valueOf(value);
        }
        throw new ParsingException("Not a number");
    }
}
