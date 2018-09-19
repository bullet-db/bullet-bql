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
import com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType;
import com.yahoo.bullet.bql.tree.AllColumns;
import com.yahoo.bullet.bql.tree.ArithmeticUnaryExpression;
import com.yahoo.bullet.bql.tree.BetweenPredicate;
import com.yahoo.bullet.bql.tree.BinaryExpression;
import com.yahoo.bullet.bql.tree.BooleanLiteral;
import com.yahoo.bullet.bql.tree.CastExpression;
import com.yahoo.bullet.bql.tree.ComparisonExpression;
import com.yahoo.bullet.bql.tree.ContainsPredicate;
import com.yahoo.bullet.bql.tree.DecimalLiteral;
import com.yahoo.bullet.bql.tree.DereferenceExpression;
import com.yahoo.bullet.bql.tree.DoubleLiteral;
import com.yahoo.bullet.bql.tree.Expression;
import com.yahoo.bullet.bql.tree.FunctionCall;
import com.yahoo.bullet.bql.tree.GroupBy;
import com.yahoo.bullet.bql.tree.GroupingElement;
import com.yahoo.bullet.bql.tree.Identifier;
import com.yahoo.bullet.bql.tree.InPredicate;
import com.yahoo.bullet.bql.tree.IsEmptyPredicate;
import com.yahoo.bullet.bql.tree.IsNotEmptyPredicate;
import com.yahoo.bullet.bql.tree.IsNotNullPredicate;
import com.yahoo.bullet.bql.tree.IsNullPredicate;
import com.yahoo.bullet.bql.tree.LeafExpression;
import com.yahoo.bullet.bql.tree.LikePredicate;
import com.yahoo.bullet.bql.tree.LinearDistribution;
import com.yahoo.bullet.bql.tree.LogicalBinaryExpression;
import com.yahoo.bullet.bql.tree.LongLiteral;
import com.yahoo.bullet.bql.tree.ManualDistribution;
import com.yahoo.bullet.bql.tree.Node;
import com.yahoo.bullet.bql.tree.NodeLocation;
import com.yahoo.bullet.bql.tree.NotExpression;
import com.yahoo.bullet.bql.tree.NullLiteral;
import com.yahoo.bullet.bql.tree.OrderBy;
import com.yahoo.bullet.bql.tree.QualifiedName;
import com.yahoo.bullet.bql.tree.Query;
import com.yahoo.bullet.bql.tree.QueryBody;
import com.yahoo.bullet.bql.tree.QuerySpecification;
import com.yahoo.bullet.bql.tree.ReferenceWithFunction;
import com.yahoo.bullet.bql.tree.RegionDistribution;
import com.yahoo.bullet.bql.tree.Relation;
import com.yahoo.bullet.bql.tree.Select;
import com.yahoo.bullet.bql.tree.SelectItem;
import com.yahoo.bullet.bql.tree.SimpleGroupBy;
import com.yahoo.bullet.bql.tree.SingleColumn;
import com.yahoo.bullet.bql.tree.SortItem;
import com.yahoo.bullet.bql.tree.Stream;
import com.yahoo.bullet.bql.tree.StringLiteral;
import com.yahoo.bullet.bql.tree.TopK;
import com.yahoo.bullet.bql.tree.ValueListExpression;
import com.yahoo.bullet.bql.tree.WindowInclude;
import com.yahoo.bullet.bql.tree.WindowInclude.IncludeType;
import com.yahoo.bullet.bql.tree.Windowing;
import com.yahoo.bullet.parsing.Clause.Operation;
import com.yahoo.bullet.parsing.Window.Unit;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.yahoo.bullet.aggregations.Distribution.Type.CDF;
import static com.yahoo.bullet.aggregations.Distribution.Type.PMF;
import static com.yahoo.bullet.aggregations.Distribution.Type.QUANTILE;
import static com.yahoo.bullet.bql.tree.WindowInclude.IncludeType.FIRST;
import static com.yahoo.bullet.parsing.Window.Unit.ALL;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

class ASTBuilder extends BQLBaseBaseVisitor<Node> {
    private final ParsingOptions parsingOptions;

    ASTBuilder(ParsingOptions parsingOptions) {
        this.parsingOptions = requireNonNull(parsingOptions, "ParsingOptions is null");
    }

    @Override
    public Node visitSingleStatement(BQLBaseParser.SingleStatementContext context) throws IllegalArgumentException,
            UnsupportedOperationException, ParsingException, NullPointerException, AssertionError {
        return visit(context.statement());
    }

    @Override
    public Node visitSingleExpression(BQLBaseParser.SingleExpressionContext context) throws IllegalArgumentException,
            UnsupportedOperationException, ParsingException, NullPointerException, AssertionError {
        return visit(context.expression());
    }

    // ******************* Statements **********************

    @Override
    public Node visitQueryNoWith(BQLBaseParser.QueryNoWithContext context) throws IllegalArgumentException,
            UnsupportedOperationException, ParsingException, NullPointerException, AssertionError {
        QueryBody term = (QueryBody) visit(context.queryTerm());
        Optional<OrderBy> orderBy = Optional.empty();
        if (context.ORDER() != null) {
            orderBy = Optional.of(new OrderBy(getLocation(context.ORDER()),
                                  visit(context.sortItem(), SortItem.class),
                                  Optional.ofNullable(context.ordering)
                                          .map(ASTBuilder::getOrderingType)
                                          .orElse(OrderBy.Ordering.ASCENDING)));
        }
        Optional<Windowing> windowing = visitIfPresent(context.windowOperation(), Windowing.class);
        QuerySpecification query = (QuerySpecification) term;

        return new Query(
                getLocation(context),
                Optional.empty(),
                new QuerySpecification(
                        getLocation(context),
                        query.getSelect(),
                        query.getFrom(),
                        query.getWhere(),
                        query.getGroupBy(),
                        query.getHaving(),
                        orderBy,
                        getTextIfPresent(context.limit),
                        windowing),
                Optional.empty(),
                Optional.empty());
    }

    @Override
    public Node visitSortItem(BQLBaseParser.SortItemContext context) {
        return new SortItem(getLocation(context),
                            (Expression) visit(context.expression()),
                            SortItem.NullOrdering.UNDEFINED);
    }

    @Override
    public Node visitEmitEvery(BQLBaseParser.EmitEveryContext context) {
        Long emitEvery = Long.parseLong(context.emitEvery.getText());
        Unit emitType = Unit.valueOf(context.emitType.getText().toUpperCase());
        WindowInclude include = (WindowInclude) visitInclude(context.include());
        return new Windowing(getLocation(context), emitEvery, emitType, include);
    }

    @Override
    public Node visitInclude(BQLBaseParser.IncludeContext context) {
        Unit unit = Unit.valueOf(context.includeUnit.getText().toUpperCase());
        Optional<IncludeType> type = Optional.empty();
        Optional<Long> number = Optional.empty();

        if (unit != ALL) {
            type = getIncludeType(context);
            number = Optional.of(Long.parseLong(context.INTEGER_VALUE().getText()));
        }

        return new WindowInclude(getLocation(context), unit, type, number);
    }

    @Override
    public Node visitTumbling(BQLBaseParser.TumblingContext context) {
        Long emitEvery = Long.parseLong(context.emitEvery.getText());
        Unit emitType = Unit.valueOf(context.emitType.getText().toUpperCase());
        WindowInclude include = new WindowInclude(
                getLocation(context),
                emitType,
                Optional.of(FIRST),
                Optional.of(emitEvery));

        return new Windowing(getLocation(context), emitEvery, emitType, include);
    }

    @Override
    public Node visitQuerySpecification(BQLBaseParser.QuerySpecificationContext context) throws IllegalArgumentException,
            UnsupportedOperationException, ParsingException, NullPointerException, AssertionError {
        List<SelectItem> selectItems = visit(context.selectItem(), SelectItem.class);
        Optional<Relation> from = Optional.of((Relation) (visit(context.relation())));

        return new QuerySpecification(
                getLocation(context),
                new Select(getLocation(context.SELECT()), isDistinct(context.setQuantifier()), selectItems),
                from,
                visitIfPresent(context.where, Expression.class),
                visitIfPresent(context.groupBy(), GroupBy.class),
                visitIfPresent(context.having, Expression.class),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    @Override
    public Node visitTopKThreshold(BQLBaseParser.TopKThresholdContext context) throws ParsingException {
        return new ComparisonExpression(
                getLocation(context),
                getComparisonOperator(((TerminalNode) context.comparisonOperator().getChild(0)).getSymbol()),
                (Expression) (visit(context.expression())),
                new LongLiteral(getLocation(context.right), context.right.getText()),
                false);
    }

    @Override
    public Node visitGroupBy(BQLBaseParser.GroupByContext context) {
        return new GroupBy(
                getLocation(context),
                false,
                visit(asList(context.groupingElement()), GroupingElement.class));
    }

    @Override
    public Node visitSingleGroupingSet(BQLBaseParser.SingleGroupingSetContext context) {
        return new SimpleGroupBy(
                getLocation(context),
                visit(context.groupingExpressions().referenceExpression(), Expression.class));
    }

    @Override
    public Node visitSelectAll(BQLBaseParser.SelectAllContext context) {
        Optional<Identifier> alias = visitIfPresent(context.identifier(), Identifier.class);
        if (context.qualifiedName() != null) {
            return new AllColumns(getLocation(context), getQualifiedName(context.qualifiedName()), alias);
        }

        return new AllColumns(getLocation(context));
    }

    @Override
    public Node visitSelectSingle(BQLBaseParser.SelectSingleContext context) {
        return new SingleColumn(
                getLocation(context),
                (Expression) visit(context.primaryExpression()),
                visitIfPresent(context.identifier(), Identifier.class));
    }

    // ***************** Boolean Expressions ******************

    @Override
    public Node visitLogicalNot(BQLBaseParser.LogicalNotContext context) {
        return new NotExpression(getLocation(context), (Expression) visit(context.booleanExpression()));
    }

    @Override
    public Node visitLogicalBinary(BQLBaseParser.LogicalBinaryContext context) throws IllegalArgumentException {
        return new LogicalBinaryExpression(
                getLocation(context.operator),
                getLogicalBinaryOperator(context.operator),
                (Expression) visit(context.left),
                (Expression) visit(context.right));
    }

    @Override
    public Node visitParenthesizedExpression(BQLBaseParser.ParenthesizedExpressionContext context) {
        return visit(context.booleanExpression());
    }

    // *************** From Clause *****************

    @Override
    public Node visitSampledRelation(BQLBaseParser.SampledRelationContext context) {
        return visit(context.aliasedRelation());
    }

    @Override
    public Node visitAliasedRelation(BQLBaseParser.AliasedRelationContext context) {
        return visit(context.relationPrimary());
    }

    @Override
    public Node visitStream(BQLBaseParser.StreamContext context) {
        Optional<String> timeDuration = getTextIfPresent(context.timeDuration);
        Optional<String> recordDuration = getTextIfPresent(context.recordDuration);
        return new Stream(getLocation(context), timeDuration, recordDuration);
    }
    // ********************* Predicates *******************

    @Override
    public Node visitPredicated(BQLBaseParser.PredicatedContext context) {
        return visit(context.predicate());
    }

    @Override
    public Node visitReferenceWithFunction(BQLBaseParser.ReferenceWithFunctionContext context) {
        return new ReferenceWithFunction(
                getLocation(context),
                getFunctionOperator(((TerminalNode) context.functionName().getChild(0)).getSymbol()),
                (Expression) visit(context.referenceExpression()));
    }

    @Override
    public Node visitComparison(BQLBaseParser.ComparisonContext context) throws IllegalArgumentException {
        return new ComparisonExpression(
                getLocation(context.comparisonOperator()),
                getComparisonOperator(((TerminalNode) context.comparisonOperator().getChild(0)).getSymbol()),
                (Expression) visit(context.value),
                (Expression) visit(context.right),
                false);
    }

    @Override
    public Node visitDistinctFrom(BQLBaseParser.DistinctFromContext context) {
        Expression expression = new ComparisonExpression(
                getLocation(context),
                Operation.NOT_EQUALS,
                (Expression) visit(context.value),
                (Expression) visit(context.right),
                true);

        if (context.NOT() != null) {
            expression = new NotExpression(getLocation(context), expression);
        }

        return expression;
    }

    @Override
    public Node visitBetween(BQLBaseParser.BetweenContext context) {
        Expression expression = new BetweenPredicate(
                getLocation(context),
                (Expression) visit(context.value),
                (Expression) visit(context.lower),
                (Expression) visit(context.upper));

        if (context.NOT() != null) {
            expression = new NotExpression(getLocation(context), expression);
        }

        return expression;
    }

    @Override
    public Node visitNullPredicate(BQLBaseParser.NullPredicateContext context) {
        Expression child = (Expression) visit(context.value);

        if (context.NOT() == null) {
            return new IsNullPredicate(getLocation(context), child);
        }

        return new IsNotNullPredicate(getLocation(context), child);
    }

    @Override
    public Node visitEmptyPredicate(BQLBaseParser.EmptyPredicateContext context) {
        Expression child = (Expression) visit(context.value);

        if (context.NOT() == null) {
            return new IsEmptyPredicate(getLocation(context), child);
        }

        return new IsNotEmptyPredicate(getLocation(context), child);
    }

    @Override
    public Node visitLikeList(BQLBaseParser.LikeListContext context) {
        Expression result = new LikePredicate(
                getLocation(context),
                (Expression) visit(context.value),
                new ValueListExpression(getLocation(context), visit(context.valueExpressionList().valueExpression(), Expression.class)));

        if (context.NOT() != null) {
            result = new NotExpression(getLocation(context), result);
        }

        return result;
    }

    @Override
    public Node visitInList(BQLBaseParser.InListContext context) {
        Expression result = new InPredicate(
                getLocation(context),
                (Expression) visit(context.value),
                new ValueListExpression(getLocation(context), visit(context.valueExpressionList().valueExpression(), Expression.class)));

        if (context.NOT() != null) {
            result = new NotExpression(getLocation(context), result);
        }

        return result;
    }

    @Override
    public Node visitContainsList(BQLBaseParser.ContainsListContext context) {
        Expression result = new ContainsPredicate(
                getLocation(context),
                getContainsOperator(((TerminalNode) context.containsOperator().getChild(0)).getSymbol()),
                (Expression) visit(context.value),
                new ValueListExpression(getLocation(context), visit(context.valueExpressionList().valueExpression(), Expression.class)));

        if (context.NOT() != null) {
            result = new NotExpression(getLocation(context), result);
        }
        return result;
    }

    // ************** Value Expressions **************

    @Override
    public Node visitArithmeticUnary(BQLBaseParser.ArithmeticUnaryContext context) {
        Expression child = (Expression) visit(context.number());
        if (context.operator.getType() == BQLBaseLexer.MINUS) {
            return ArithmeticUnaryExpression.negative(getLocation(context), child);
        } else {
            return ArithmeticUnaryExpression.positive(getLocation(context), child);
        }
    }

    // ********************* Primary Expressions **********************

    @Override
    public Node visitDereference(BQLBaseParser.DereferenceContext context) {
        return new DereferenceExpression(
                getLocation(context),
                (Expression) visit(context.base),
                (Identifier) visit(context.fieldName));
    }

    @Override
    public Node visitColumnReference(BQLBaseParser.ColumnReferenceContext context) {
        return visit(context.identifier());
    }

    @Override
    public Node visitFunctionCall(BQLBaseParser.FunctionCallContext context) {
        return new FunctionCall(
                getLocation(context),
                getGroupOperationType(context),
                Optional.empty(),
                Optional.empty(),
                isDistinct(context.setQuantifier()),
                visit(context.referenceExpression(), Expression.class));
    }

    @Override
    public Node visitUnquotedIdentifier(BQLBaseParser.UnquotedIdentifierContext context) {
        return new Identifier(getLocation(context), context.getText(), false);
    }

    @Override
    public Node visitDistributionOperation(BQLBaseParser.DistributionOperationContext context) throws IllegalArgumentException, UnsupportedOperationException {
        List<Expression> columns = visit(asList(context.referenceExpression()), Expression.class);
        Type type = getDistributionType(context);
        NodeLocation location = getLocation(context);
        String iMode = context.inputMode().iMode.getText().toUpperCase();

        switch (iMode) {
            case "LINEAR":
                Long numberOfPoints = Long.parseLong(context.inputMode().numberOfPoints.getText());
                return new LinearDistribution(location, columns, type, numberOfPoints);
            case "REGION":
                Double start = getSignedDouble(context.inputMode().start);
                Double end = getSignedDouble(context.inputMode().end);
                Double increment = getSignedDouble(context.inputMode().increment);
                return new RegionDistribution(location, columns, type, start, end, increment);
            case "MANUAL":
                List<Double> points = new ArrayList<>();
                for (BQLBaseParser.SignedNumberContext t : context.inputMode().signedNumber()) {
                    points.add(getSignedDouble(t));
                }
                return new ManualDistribution(location, columns, type, points);
        }

        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Node visitTopK(BQLBaseParser.TopKContext context) {
        return visit(context.topKConfig());
    }

    @Override
    public Node visitTopKConfig(BQLBaseParser.TopKConfigContext context) {
        List<Expression> columns = visit(context.referenceExpression(), Expression.class);
        Long size = Long.parseLong(context.size.getText());
        Optional<Long> threshold = Optional.empty();
        if (context.threshold != null) {
            threshold = Optional.of(Long.parseLong(context.threshold.getText()));
        }
        return new TopK(getLocation(context), columns, size, threshold);
    }

    @Override
    public Node visitCastExpression(BQLBaseParser.CastExpressionContext context) {
        BQLBaseParser.CastTypeContext castTypeContext = context.castType();
        return new CastExpression(getLocation(context),
                                  (Expression) visit(context.arithmeticExpression()),
                                  castTypeContext != null ? castTypeContext.getText() : null);
    }

    @Override
    public Node visitBinaryExpression(BQLBaseParser.BinaryExpressionContext context) {
        return new BinaryExpression(getLocation(context),
                                    (Expression) visit(context.left),
                                    (Expression) visit(context.right),
                                    context.op.getText());
    }

    @Override
    public Node visitLeafExpression(BQLBaseParser.LeafExpressionContext context) {
        return new LeafExpression(getLocation(context),
                                  (Expression) visit(context.valueExpression()));
    }

    @Override
    public Node visitParensExpression(BQLBaseParser.ParensExpressionContext context) {
        return visit(context.arithmeticExpression());
    }

    // ************** Literals **************

    @Override
    public Node visitNullLiteral(BQLBaseParser.NullLiteralContext context) {
        return new NullLiteral(getLocation(context));
    }

    @Override
    public Node visitBasicStringLiteral(BQLBaseParser.BasicStringLiteralContext context) {
        return new StringLiteral(getLocation(context), unquote(context.STRING().getText()));
    }

    @Override
    public Node visitIntegerLiteral(BQLBaseParser.IntegerLiteralContext context) {
        return new LongLiteral(getLocation(context), context.getText());
    }

    @Override
    public Node visitDecimalLiteral(BQLBaseParser.DecimalLiteralContext context) throws ParsingException, AssertionError {
        switch (parsingOptions.getDecimalLiteralTreatment()) {
            case AS_DOUBLE:
                return new DoubleLiteral(getLocation(context), context.getText());
            case AS_DECIMAL:
                return new DecimalLiteral(getLocation(context), context.getText());
            case REJECT:
                throw parseError("Unexpected decimal literal: " + context.getText(), context);
        }

        throw new AssertionError("Unreachable");
    }

    @Override
    public Node visitDoubleLiteral(BQLBaseParser.DoubleLiteralContext context) {
        return new DoubleLiteral(getLocation(context), context.getText());
    }

    @Override
    public Node visitBooleanValue(BQLBaseParser.BooleanValueContext context) {
        return new BooleanLiteral(getLocation(context), context.getText());
    }

    // ***************** Helpers *****************

    @Override
    protected Node defaultResult() {
        return null;
    }

    @Override
    protected Node aggregateResult(Node aggregate, Node nextResult) throws UnsupportedOperationException {
        if (nextResult == null) {
            throw new UnsupportedOperationException("Not yet implemented");
        }
        if (aggregate == null) {
            return nextResult;
        }
        throw new UnsupportedOperationException("Not yet implemented");
    }

    private static OrderBy.Ordering getOrderingType(Token token) throws IllegalArgumentException {
        if (token.getType() == BQLBaseLexer.DESC) {
            return OrderBy.Ordering.DESCENDING;
        } else if (token.getType() == BQLBaseLexer.ASC) {
            return OrderBy.Ordering.ASCENDING;
        }
        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }

    private GroupOperationType getGroupOperationType(BQLBaseParser.FunctionCallContext context) {
        String functionName = context.qualifiedName().getText().toUpperCase();
        return GroupOperationType.valueOf(functionName);
    }

    private <T> Optional<T> visitIfPresent(ParserRuleContext context, Class<T> clazz) {
        return Optional.ofNullable(context)
                .map(this::visit)
                .map(clazz::cast);
    }

    private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> clazz) {
        return contexts.stream()
                .map(this::visit)
                .map(clazz::cast)
                .collect(toList());
    }

    private static String unquote(String value) {
        return value.substring(1, value.length() - 1)
                .replace("''", "'");
    }

    private QualifiedName getQualifiedName(BQLBaseParser.QualifiedNameContext context) {
        List<String> parts = visit(asList(context.identifier()), Identifier.class).stream()
                .map(Identifier::getValue) // TODO: preserve quotedness
                .collect(Collectors.toList());

        return QualifiedName.of(parts);
    }

    private static boolean isDistinct(BQLBaseParser.SetQuantifierContext setQuantifier) {
        return setQuantifier != null;
    }

    private static Optional<String> getTextIfPresent(Token token) {
        return Optional.ofNullable(token)
                .map(Token::getText);
    }

    private Optional<IncludeType> getIncludeType(BQLBaseParser.IncludeContext context) {
        return Optional.of(IncludeType.valueOf(context.includeType.getText().toUpperCase()));
    }

    private static Operation getComparisonOperator(Token symbol) throws IllegalArgumentException {
        Operation op = null;
        switch (symbol.getType()) {
            case BQLBaseLexer.EQ:
                op = Operation.EQUALS;
                break;
            case BQLBaseLexer.NEQ:
                op = Operation.NOT_EQUALS;
                break;
            case BQLBaseLexer.LT:
                op = Operation.LESS_THAN;
                break;
            case BQLBaseLexer.LTE:
                op = Operation.LESS_EQUALS;
                break;
            case BQLBaseLexer.GT:
                op = Operation.GREATER_THAN;
                break;
            case BQLBaseLexer.GTE:
                op = Operation.GREATER_EQUALS;
                break;
        }
        return op;
    }

    private static Operation getContainsOperator(Token symbol) throws IllegalArgumentException {
        Operation op = null;
        switch (symbol.getType()) {
            case BQLBaseLexer.CONTAINSKEY:
                op = Operation.CONTAINS_KEY;
                break;
            case BQLBaseLexer.CONTAINSVALUE:
                op = Operation.CONTAINS_VALUE;
                break;
        }
        return op;
    }

    private static Operation getFunctionOperator(Token symbol) throws IllegalArgumentException {
        Operation op = null;
        switch (symbol.getType()) {
            case BQLBaseLexer.SIZEOF:
                op = Operation.SIZE_IS;
                break;
        }
        return op;
    }

    private Type getDistributionType(BQLBaseParser.DistributionOperationContext context) throws IllegalArgumentException {
        String typeString = context.distributionType().getText().toUpperCase();
        Type type = null;
        switch (typeString) {
            case "QUANTILE":
                type = QUANTILE;
                break;
            case "FREQ":
                type = PMF;
                break;
            case "CUMFREQ":
                type = CDF;
                break;
        }
        return type;
    }

    private static Operation getLogicalBinaryOperator(Token token) throws IllegalArgumentException {
        Operation op = null;
        switch (token.getType()) {
            case BQLBaseLexer.AND:
                op = Operation.AND;
                break;
            case BQLBaseLexer.OR:
                op = Operation.OR;
                break;
        }
        return op;
    }

    private static NodeLocation getLocation(TerminalNode terminalNode) {
        requireNonNull(terminalNode, "TerminalNode is null");
        return getLocation(terminalNode.getSymbol());
    }

    private static NodeLocation getLocation(ParserRuleContext parserRuleContext) {
        requireNonNull(parserRuleContext, "ParserRuleContext is null");
        return getLocation(parserRuleContext.getStart());
    }

    private static NodeLocation getLocation(Token token) {
        requireNonNull(token, "Token is null");
        return new NodeLocation(token.getLine(), token.getCharPositionInLine());
    }

    private static ParsingException parseError(String message, ParserRuleContext context) {
        return new ParsingException(
                message,
                null,
                context.getStart().getLine(),
                context.getStart().getCharPositionInLine());
    }

    private double getSignedDouble(BQLBaseParser.SignedNumberContext context) {
        double number = Double.parseDouble(context.number().getText());
        if (context.operator != null && context.operator.getText().equals("-")) {
            return -number;
        } else {
            return number;
        }
    }
}
