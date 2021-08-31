/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/parser/AstBuilder.java
 */
package com.yahoo.bullet.bql.parser;

import com.yahoo.bullet.bql.tree.BetweenPredicateNode;
import com.yahoo.bullet.bql.tree.CastExpressionNode;
import com.yahoo.bullet.bql.tree.CountDistinctNode;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.FieldExpressionNode;
import com.yahoo.bullet.bql.tree.GroupByNode;
import com.yahoo.bullet.bql.tree.GroupOperationNode;
import com.yahoo.bullet.bql.tree.IdentifierNode;
import com.yahoo.bullet.bql.tree.BinaryExpressionNode;
import com.yahoo.bullet.bql.tree.LateralViewNode;
import com.yahoo.bullet.bql.tree.LinearDistributionNode;
import com.yahoo.bullet.bql.tree.ListExpressionNode;
import com.yahoo.bullet.bql.tree.LiteralNode;
import com.yahoo.bullet.bql.tree.ManualDistributionNode;
import com.yahoo.bullet.bql.tree.NAryExpressionNode;
import com.yahoo.bullet.bql.tree.Node;
import com.yahoo.bullet.bql.tree.NodeLocation;
import com.yahoo.bullet.bql.tree.NullPredicateNode;
import com.yahoo.bullet.bql.tree.OrderByNode;
import com.yahoo.bullet.bql.tree.ParenthesesExpressionNode;
import com.yahoo.bullet.bql.tree.QueryNode;
import com.yahoo.bullet.bql.tree.RegionDistributionNode;
import com.yahoo.bullet.bql.tree.SelectNode;
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.bql.tree.SortItemNode;
import com.yahoo.bullet.bql.tree.StreamNode;
import com.yahoo.bullet.bql.tree.SubFieldExpressionNode;
import com.yahoo.bullet.bql.tree.TableFunctionNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.bql.tree.UnaryExpressionNode;
import com.yahoo.bullet.bql.tree.WindowIncludeNode;
import com.yahoo.bullet.bql.tree.WindowNode;
import com.yahoo.bullet.query.Window.Unit;
import com.yahoo.bullet.query.aggregations.DistributionType;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.query.tablefunctions.TableFunctionType;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.typesystem.Type;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.yahoo.bullet.query.aggregations.DistributionType.CDF;
import static com.yahoo.bullet.query.aggregations.DistributionType.PMF;
import static com.yahoo.bullet.query.aggregations.DistributionType.QUANTILE;

class ASTBuilder extends BQLBaseBaseVisitor<Node> {
    @Override
    public Node visitQuery(BQLBaseParser.QueryContext context) {
        return new QueryNode((SelectNode) visit(context.select()),
                             (StreamNode) visit(context.stream()),
                             (LateralViewNode) visitIfPresent(context.lateralView()),
                             stripParentheses((ExpressionNode) visitIfPresent(context.where)),
                             (GroupByNode) visitIfPresent(context.groupBy()),
                             stripParentheses((ExpressionNode) visitIfPresent(context.having)),
                             (OrderByNode) visitIfPresent(context.orderBy()),
                             (WindowNode) visitIfPresent(context.window()),
                             getTextIfPresent(context.limit),
                             getLocation(context));
    }

    @Override
    public Node visitSelect(BQLBaseParser.SelectContext context) {
        return new SelectNode(context.DISTINCT() != null,
                              visit(context.selectItem(), SelectItemNode.class),
                              getLocation(context));
    }

    @Override
    public Node visitSelectItem(BQLBaseParser.SelectItemContext context) {
        if (context.expression() != null) {
            return new SelectItemNode(false,
                                      stripParentheses((ExpressionNode) visit(context.expression())),
                                      (IdentifierNode) visitIfPresent(context.identifier()),
                                      getLocation(context));
        } else if (context.tableFunction() != null) {
            return new SelectItemNode(false,
                                      (ExpressionNode) visit(context.tableFunction()),
                                      null,
                                      getLocation(context));
        }
        return new SelectItemNode(true, null, null, getLocation(context));
    }

    @Override
    public Node visitStream(BQLBaseParser.StreamContext context) {
        return new StreamNode(getTextIfPresent(context.timeDuration), getLocation(context));
    }

    @Override
    public Node visitLateralView(BQLBaseParser.LateralViewContext context) {
        return new LateralViewNode(visit(context.tableFunction(), TableFunctionNode.class), getLocation(context));
    }

    @Override
    public Node visitGroupBy(BQLBaseParser.GroupByContext context) {
        return new GroupByNode(visitExpressionsList(context.expressions()).stream().map(this::stripParentheses).collect(Collectors.toList()),
                               getLocation(context));
    }

    @Override
    public Node visitOrderBy(BQLBaseParser.OrderByContext context) {
        return new OrderByNode(visit(context.sortItem(), SortItemNode.class), getLocation(context));
    }

    @Override
    public Node visitSortItem(BQLBaseParser.SortItemContext context) {
        return new SortItemNode(stripParentheses((ExpressionNode) visit(context.expression())),
                                getOrdering(context.ordering),
                                getLocation(context));
    }

    @Override
    public Node visitWindow(BQLBaseParser.WindowContext context) {
        Integer emitEvery = Integer.parseInt(context.emitEvery.getText());
        Unit emitType = Unit.valueOf(context.emitType.getText().toUpperCase());
        return new WindowNode(emitEvery,
                              emitType,
                              (WindowIncludeNode) visitIfPresent(context.include()),
                              getLocation(context));
    }

    @Override
    public Node visitInclude(BQLBaseParser.IncludeContext context) {
        Integer first = context.INTEGER_VALUE() != null ? Integer.parseInt(context.INTEGER_VALUE().getText()) : null;
        Unit includeUnit = Unit.valueOf(context.includeUnit.getText().toUpperCase());
        return new WindowIncludeNode(first, includeUnit, getLocation(context));
    }

    @Override
    public Node visitFieldExpression(BQLBaseParser.FieldExpressionContext context) {
        IdentifierNode field = (IdentifierNode) visit(context.field);
        NodeLocation location = getLocation(context);
        return new FieldExpressionNode(field, null, location);
    }

    @Override
    public Node visitSubFieldExpression(BQLBaseParser.SubFieldExpressionContext context) {
        FieldExpressionNode field = (FieldExpressionNode) visit(context.field);
        Integer index = context.index != null ? Integer.valueOf(context.index.getText()) : null;
        IdentifierNode key = (IdentifierNode) visitIfPresent(context.key);
        ExpressionNode expressionKey = (ExpressionNode) visitIfPresent(context.expressionKey);
        String stringKey = getTextIfPresent(context.stringKey);
        NodeLocation location = getLocation(context);
        return new SubFieldExpressionNode(field, index, key, expressionKey, stringKey != null ? unquoteSingle(stringKey) : null, null, location);
    }

    @Override
    public Node visitSubSubFieldExpression(BQLBaseParser.SubSubFieldExpressionContext context) {
        SubFieldExpressionNode field = (SubFieldExpressionNode) visit(context.subField);
        IdentifierNode key = (IdentifierNode) visitIfPresent(context.key);
        ExpressionNode expressionKey = (ExpressionNode) visitIfPresent(context.expressionKey);
        String stringKey = getTextIfPresent(context.stringKey);
        NodeLocation location = getLocation(context);
        return new SubFieldExpressionNode(field, null, key, expressionKey, stringKey != null ? unquoteSingle(stringKey) : null, null, location);
    }

    @Override
    public Node visitField(BQLBaseParser.FieldContext context) {
        FieldExpressionNode field = (FieldExpressionNode) visit(context.fieldExpression());
        Type type = getType(context.fieldType());
        if (type != null) {
            field.setType(type);
        }
        return field;
    }

    @Override
    public Node visitSubField(BQLBaseParser.SubFieldContext context) {
        SubFieldExpressionNode subField = (SubFieldExpressionNode) visit(context.subFieldExpression());
        Type type = getType(context.fieldType());
        if (type != null) {
            FieldExpressionNode field = (FieldExpressionNode) subField.getField();
            field.setType(type);
        }
        return subField;
    }

    @Override
    public Node visitSubSubField(BQLBaseParser.SubSubFieldContext context) {
        SubFieldExpressionNode subSubField = (SubFieldExpressionNode) visit(context.subSubFieldExpression());
        Type type = getType(context.fieldType());
        if (type != null) {
            SubFieldExpressionNode subField = (SubFieldExpressionNode) subSubField.getField();
            FieldExpressionNode field = (FieldExpressionNode) subField.getField();
            field.setType(type);
        }
        return subSubField;
    }

    @Override
    public Node visitListExpression(BQLBaseParser.ListExpressionContext context) {
        return new ListExpressionNode(visitExpressionsList(context.expressions()), getLocation(context));
    }

    @Override
    public Node visitNullPredicate(BQLBaseParser.NullPredicateContext context) {
        return new NullPredicateNode((ExpressionNode) visit(context.expression()),
                                     context.NOT() != null,
                                     getLocation(context));
    }

    @Override
    public Node visitBetweenPredicate(BQLBaseParser.BetweenPredicateContext context) {
        return new BetweenPredicateNode((ExpressionNode) visit(context.value),
                                        (ExpressionNode) visit(context.lower),
                                        (ExpressionNode) visit(context.upper),
                                        context.NOT() != null,
                                        getLocation(context));
    }

    @Override
    public Node visitUnaryExpression(BQLBaseParser.UnaryExpressionContext context) {
        return new UnaryExpressionNode(getOperation(context.op),
                                       (ExpressionNode) visit(context.expression()),
                                       context.parens != null,
                                       getLocation(context));
    }

    @Override
    public Node visitBinary(BQLBaseParser.BinaryContext context) {
        return new BinaryExpressionNode((ExpressionNode) visit(context.left),
                                        (ExpressionNode) visit(context.right),
                                        getOperation(context.op),
                                        getLocation(context));
    }

    @Override
    public Node visitNAry(BQLBaseParser.NAryContext context) {
        return new NAryExpressionNode(getOperation(context.op),
                                      visitExpressionsList(context.expressions()),
                                      getLocation(context));
    }

    @Override
    public Node visitGroupOperation(BQLBaseParser.GroupOperationContext context) {
        GroupOperation.GroupOperationType op = GroupOperation.GroupOperationType.valueOf(context.op.getText().toUpperCase());
        return new GroupOperationNode(op, (ExpressionNode) visitIfPresent(context.expression()), getLocation(context));
    }

    @Override
    public Node visitCountDistinct(BQLBaseParser.CountDistinctContext context) {
        return new CountDistinctNode(visitExpressionsList(context.expressions()), getLocation(context));
    }

    @Override
    public Node visitDistribution(BQLBaseParser.DistributionContext context) {
        DistributionType type = getDistributionType(context);
        ExpressionNode expression = (ExpressionNode) visit(context.expression());
        switch (context.inputMode().iMode.getType()) {
            case BQLBaseLexer.LINEAR: {
                int numberOfPoints = Integer.parseInt(context.inputMode().numberOfPoints.getText());
                return new LinearDistributionNode(type, expression, numberOfPoints, getLocation(context));
            }
            case BQLBaseLexer.REGION: {
                double start = getSignedNumber(context.inputMode().start).doubleValue();
                double end = getSignedNumber(context.inputMode().end).doubleValue();
                double increment = getSignedNumber(context.inputMode().increment).doubleValue();
                return new RegionDistributionNode(type, expression, start, end, increment, getLocation(context));
            }
            case BQLBaseLexer.MANUAL: {
                List<Double> points = context.inputMode().number().stream().map(this::getSignedNumber).map(Number::doubleValue).collect(Collectors.toList());
                return new ManualDistributionNode(type, expression, points, getLocation(context));
            }
        }
        throw new ParsingException("Unknown input mode");
    }

    @Override
    public Node visitTopK(BQLBaseParser.TopKContext context) {
        Integer size = Integer.parseInt(context.size.getText());
        Long threshold = context.threshold != null ? Long.parseLong(context.threshold.getText()) : null;
        return new TopKNode(size, threshold, visitExpressionsList(context.expressions()), getLocation(context));
    }

    @Override
    public Node visitCast(BQLBaseParser.CastContext context) {
        Type castType = Type.valueOf(context.primitiveType().getText().toUpperCase());
        return new CastExpressionNode((ExpressionNode) visit(context.expression()), castType, getLocation(context));
    }

    @Override
    public Node visitTableFunction(BQLBaseParser.TableFunctionContext context) {
        if (context.op.getType() == BQLBaseLexer.EXPLODE) {
            return new TableFunctionNode(TableFunctionType.EXPLODE,
                                         (ExpressionNode) visit(context.expression()),
                                         (IdentifierNode) visit(context.keyAlias),
                                         (IdentifierNode) visitIfPresent(context.valueAlias),
                                         context.OUTER() != null,
                                         getLocation(context));
        }
        throw new ParsingException("Unknown table function");
    }

    @Override
    public Node visitInfix(BQLBaseParser.InfixContext context) {
        return new BinaryExpressionNode((ExpressionNode) visit(context.left),
                                        (ExpressionNode) visit(context.right),
                                        getOperation(context.op, context.modifier, context.NOT() != null),
                                        getLocation(context));
    }

    @Override
    public Node visitInfixIn(BQLBaseParser.InfixInContext context) {
        ExpressionNode rightNode;
        if (context.right != null) {
            // If the right operand is a parentheses-wrapped expression, treat it as a singleton list.
            if (context.right instanceof BQLBaseParser.ParenthesesContext) {
                ExpressionNode innerNode = (ExpressionNode) visit(((BQLBaseParser.ParenthesesContext) context.right).expression());
                rightNode = new ListExpressionNode(Collections.singletonList(innerNode), true, getLocation(context.right));
            } else {
                rightNode = (ExpressionNode) visit(context.right);
            }
        } else {
            rightNode = new ListExpressionNode(visitExpressionsList(context.expressions()), true, getLocation(context.expressions()));
        }
        return new BinaryExpressionNode((ExpressionNode) visit(context.left),
                                        rightNode,
                                        getOperation(context.op, null, context.NOT() != null),
                                        getLocation(context));
    }

    @Override
    public Node visitBooleanExpression(BQLBaseParser.BooleanExpressionContext context) {
        return new BinaryExpressionNode((ExpressionNode) visit(context.left),
                                        (ExpressionNode) visit(context.right),
                                        getOperation(context.op, null, false),
                                        getLocation(context));
    }

    @Override
    public Node visitParentheses(BQLBaseParser.ParenthesesContext context) {
        BQLBaseParser.ExpressionContext expressionContext = context.expression();
        ExpressionNode expression = (ExpressionNode) visit(expressionContext);
        // TODO add another parser rule so we only use one instanceof
        if (expressionContext instanceof BQLBaseParser.InfixContext ||
            expressionContext instanceof BQLBaseParser.NullPredicateContext ||
            expressionContext instanceof BQLBaseParser.BetweenPredicateContext) {
            return new ParenthesesExpressionNode(expression, getLocation(context));
        }
        return expression;
    }

    @Override
    public Node visitUnquotedIdentifier(BQLBaseParser.UnquotedIdentifierContext context) {
        return new IdentifierNode(context.getText(), false, getLocation(context));
    }

    @Override
    public Node visitQuotedIdentifier(BQLBaseParser.QuotedIdentifierContext context) {
        return new IdentifierNode(unquoteDouble(context.getText()), true, getLocation(context));
    }

    // ************** Literals **************

    @Override
    public Node visitNullLiteral(BQLBaseParser.NullLiteralContext context) {
        return new LiteralNode(null, getLocation(context));
    }

    @Override
    public Node visitNowLiteral(BQLBaseParser.NowLiteralContext context) {
        return new LiteralNode(System.currentTimeMillis(), getLocation(context));
    }

    @Override
    public Node visitNumericLiteral(BQLBaseParser.NumericLiteralContext context) {
        return new LiteralNode(getSignedNumber(context.number()), getLocation(context));
    }

    @Override
    public Node visitBooleanValue(BQLBaseParser.BooleanValueContext context) {
        return new LiteralNode(Boolean.valueOf(context.getText()), getLocation(context));
    }

    @Override
    public Node visitStringLiteral(BQLBaseParser.StringLiteralContext context) {
        return new LiteralNode(unquoteSingle(context.getText()), getLocation(context));
    }

    // ***************** Helpers *****************

    private ExpressionNode stripParentheses(ExpressionNode expression) {
        if (expression instanceof ParenthesesExpressionNode) {
            return ((ParenthesesExpressionNode) expression).getExpression();
        }
        return expression;
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

    private List<ExpressionNode> visitExpressionsList(BQLBaseParser.ExpressionsContext context) {
        if (context == null) {
            return Collections.emptyList();
        }
        return visit(context.expression(), ExpressionNode.class);
    }

    private static String unquoteSingle(String value) {
        // '' -> '
        return value.substring(1, value.length() - 1).replace("''", "'");
    }

    private static String unquoteDouble(String value) {
        // "" -> "
        return value.substring(1, value.length() - 1).replace("\"\"", "\"");
    }

    private static String getTextIfPresent(Token token) {
        return token != null ? token.getText() : null;
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
            case BQLBaseLexer.PERCENT:
                return Operation.MOD;
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
            case BQLBaseLexer.IN:
                return Operation.IN;
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
            case BQLBaseLexer.TRIM:
                return Operation.TRIM;
            case BQLBaseLexer.ABS:
                return Operation.ABS;
            case BQLBaseLexer.IF:
                return Operation.IF;
            case BQLBaseLexer.BETWEEN:
                return Operation.BETWEEN;
            case BQLBaseLexer.SUBSTRING:
                return Operation.SUBSTRING;
            case BQLBaseLexer.UNIXTIMESTAMP:
                return Operation.UNIX_TIMESTAMP;
            case BQLBaseLexer.LOWER:
                return Operation.LOWER;
            case BQLBaseLexer.UPPER:
                return Operation.UPPER;
        }
        return null;
    }

    private static Operation getOperation(Token op, Token modifier, boolean not) {
        if (modifier == null) {
            if (not) {
                if (op.getType() == BQLBaseLexer.RLIKE) {
                    return Operation.NOT_REGEX_LIKE;
                } else if (op.getType() == BQLBaseLexer.IN) {
                    return Operation.NOT_IN;
                }
            }
            return getOperation(op);
        }
        if (modifier.getType() == BQLBaseLexer.ANY) {
            switch (op.getType()) {
                case BQLBaseLexer.EQ:
                    return Operation.EQUALS_ANY;
                case BQLBaseLexer.NEQ:
                    return Operation.NOT_EQUALS_ANY;
                case BQLBaseLexer.GT:
                    return Operation.GREATER_THAN_ANY;
                case BQLBaseLexer.LT:
                    return Operation.LESS_THAN_ANY;
                case BQLBaseLexer.GTE:
                    return Operation.GREATER_THAN_OR_EQUALS_ANY;
                case BQLBaseLexer.LTE:
                    return Operation.LESS_THAN_OR_EQUALS_ANY;
                case BQLBaseLexer.RLIKE:
                    return not ? Operation.NOT_REGEX_LIKE_ANY : Operation.REGEX_LIKE_ANY;
            }
        } else if (modifier.getType() == BQLBaseLexer.ALL) {
            switch (op.getType()) {
                case BQLBaseLexer.EQ:
                    return Operation.EQUALS_ALL;
                case BQLBaseLexer.NEQ:
                    return Operation.NOT_EQUALS_ALL;
                case BQLBaseLexer.GT:
                    return Operation.GREATER_THAN_ALL;
                case BQLBaseLexer.LT:
                    return Operation.LESS_THAN_ALL;
                case BQLBaseLexer.GTE:
                    return Operation.GREATER_THAN_OR_EQUALS_ALL;
                case BQLBaseLexer.LTE:
                    return Operation.LESS_THAN_OR_EQUALS_ALL;
            }
        }
        return getOperation(op);
    }

    private DistributionType getDistributionType(BQLBaseParser.DistributionContext context) {
        switch (context.distributionType().type.getType()) {
            case BQLBaseLexer.QUANTILE:
                return QUANTILE;
            case BQLBaseLexer.FREQ:
                return PMF;
            case BQLBaseLexer.CUMFREQ:
                return CDF;
        }
        throw parseError("Unknown distribution type", context);
    }

    private Number getSignedNumber(BQLBaseParser.NumberContext context) {
        boolean negative = context.MINUS() != null;
        String value = context.value.getText();
        switch (context.value.getType()) {
            case BQLBaseLexer.INTEGER_VALUE:
                try {
                    return negative ? -Integer.valueOf(value) : Integer.valueOf(value);
                } catch (NumberFormatException e) {
                    return negative ? -Long.valueOf(value) : Long.valueOf(value);
                }
            case BQLBaseLexer.LONG_VALUE:
                value = value.substring(0, value.length() - 1);
                return negative ? -Long.valueOf(value) : Long.valueOf(value);
            case BQLBaseLexer.FLOAT_VALUE:
                return negative ? -Float.valueOf(value) : Float.valueOf(value);
            case BQLBaseLexer.DOUBLE_VALUE:
                return negative ? -Double.valueOf(value) : Double.valueOf(value);
        }
        throw parseError("Not a number", context);
    }

    private Type getType(BQLBaseParser.FieldTypeContext context) {
        if (context == null) {
            return null;
        }
        Type primitiveType = Type.valueOf(context.primitiveType().getText().toUpperCase());
        if (context.outerType != null) {
            if (context.outerType.getType() == BQLBaseLexer.LIST_TYPE) {
                return Type.PRIMITIVE_LISTS.stream().filter(type -> primitiveType.equals(type.getSubType())).findFirst().get();
            } else {
                return Type.PRIMITIVE_MAPS.stream().filter(type -> primitiveType.equals(type.getSubType())).findFirst().get();
            }
        }
        if (context.complexOuterType != null) {
            Type subType = Type.PRIMITIVE_MAPS.stream().filter(type -> primitiveType.equals(type.getSubType())).findFirst().get();
            if (context.complexOuterType.getType() == BQLBaseLexer.LIST_TYPE) {
                return Type.COMPLEX_LISTS.stream().filter(type -> subType.equals(type.getSubType())).findFirst().get();
            } else {
                return Type.COMPLEX_MAPS.stream().filter(type -> subType.equals(type.getSubType())).findFirst().get();
            }
        }
        return primitiveType;
    }

    private static NodeLocation getLocation(ParserRuleContext parserRuleContext) {
        Objects.requireNonNull(parserRuleContext, "ParserRuleContext is null");
        return getLocation(parserRuleContext.getStart());
    }

    private static NodeLocation getLocation(Token token) {
        Objects.requireNonNull(token, "Token is null");
        return new NodeLocation(token.getLine(), token.getCharPositionInLine());
    }

    private static ParsingException parseError(String message, ParserRuleContext context) {
        return new ParsingException(message,
                                    null,
                                    context.getStart().getLine(),
                                    context.getStart().getCharPositionInLine());
    }
}
