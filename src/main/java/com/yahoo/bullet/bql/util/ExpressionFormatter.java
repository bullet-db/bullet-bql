/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/ExpressionFormatter.java
 */
package com.yahoo.bullet.bql.util;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.yahoo.bullet.bql.tree.ArithmeticUnaryExpression;
import com.yahoo.bullet.bql.tree.ASTVisitor;
import com.yahoo.bullet.bql.tree.BetweenPredicate;
import com.yahoo.bullet.bql.tree.InfixExpression;
import com.yahoo.bullet.bql.tree.BooleanLiteral;
import com.yahoo.bullet.bql.tree.CastExpression;
import com.yahoo.bullet.bql.tree.ComparisonExpression;
import com.yahoo.bullet.bql.tree.ContainsPredicate;
import com.yahoo.bullet.bql.tree.DecimalLiteral;
import com.yahoo.bullet.bql.tree.DereferenceExpression;
import com.yahoo.bullet.bql.tree.Distribution;
import com.yahoo.bullet.bql.tree.DoubleLiteral;
import com.yahoo.bullet.bql.tree.Expression;
import com.yahoo.bullet.bql.tree.FunctionCall;
import com.yahoo.bullet.bql.tree.GroupingElement;
import com.yahoo.bullet.bql.tree.Identifier;
import com.yahoo.bullet.bql.tree.InPredicate;
import com.yahoo.bullet.bql.tree.IsNotNullPredicate;
import com.yahoo.bullet.bql.tree.IsNullPredicate;
import com.yahoo.bullet.bql.tree.LikePredicate;
import com.yahoo.bullet.bql.tree.LogicalBinaryExpression;
import com.yahoo.bullet.bql.tree.LongLiteral;
import com.yahoo.bullet.bql.tree.Node;
import com.yahoo.bullet.bql.tree.NotExpression;
import com.yahoo.bullet.bql.tree.NullLiteral;
import com.yahoo.bullet.bql.tree.OrderBy;
import com.yahoo.bullet.bql.tree.ParensExpression;
import com.yahoo.bullet.bql.tree.ReferenceWithFunction;
import com.yahoo.bullet.bql.tree.SimpleGroupBy;
import com.yahoo.bullet.bql.tree.SortItem;
import com.yahoo.bullet.bql.tree.Stream;
import com.yahoo.bullet.bql.tree.StringLiteral;
import com.yahoo.bullet.bql.tree.TopK;
import com.yahoo.bullet.bql.tree.ValueListExpression;
import com.yahoo.bullet.bql.tree.WindowInclude;
import com.yahoo.bullet.bql.tree.Windowing;
import com.yahoo.bullet.parsing.Window.Unit;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.yahoo.bullet.bql.tree.ArithmeticUnaryExpression.Sign.MINUS;
import static com.yahoo.bullet.parsing.Window.Unit.ALL;
import static java.lang.String.format;

public final class ExpressionFormatter {
    private static final ThreadLocal<DecimalFormat> DOUBLE_FORMATTER = ThreadLocal.withInitial(
            () -> new DecimalFormat("0.###################E0###", new DecimalFormatSymbols(Locale.US)));

    /**
     * Parse an {@link Expression} to a formatted BQL String, with a List of {@link Expression} parameters.
     *
     * @param expression A non-null {@link Expression} will be parsed.
     * @param parameters A List of {@link Expression} parameters that tunes the parsing.
     * @return A formatted BQL String represents the passed in {@link Expression}.
     */
    public static String formatExpression(Expression expression, Optional<List<Expression>> parameters) throws IllegalArgumentException, UnsupportedOperationException {
        return formatExpression(expression, parameters, true);
    }

    /**
     * Parse an {@link Expression} to a BQL String, with a List of {@link Expression} parameters.
     * For {@link com.yahoo.bullet.bql.tree.Literal}, the BQL String can be generated with or without format.
     *
     * @param expression A non-null {@link Expression} will be parsed.
     * @param parameters A List of {@link Expression} parameters that tunes the parsing.
     * @param withFormat A boolean which decides if the parsed BQL String of {@link com.yahoo.bullet.bql.tree.Literal} has format or not.
     * @return A BQL String represents the passed in {@link Expression}.
     */
    public static String formatExpression(Expression expression, Optional<List<Expression>> parameters, boolean withFormat) throws IllegalArgumentException, UnsupportedOperationException {
        if (expression == null) {
            throw new IllegalArgumentException("Expression inside formatExpression() must not be null");
        }

        return new Formatter(parameters, withFormat).process(expression, null);
    }

    public static class Formatter extends ASTVisitor<String, Void> {
        private final Optional<List<Expression>> parameters;
        private boolean withFormat;

        /**
         * Constructor that requires a List of {@link Expression} parameters.
         *
         * @param parameters A List of {@link Expression} parameters that tunes the parsing.
         */
        public Formatter(Optional<List<Expression>> parameters) {
            this(parameters, true);
        }

        /**
         * Constructor that requires a List of {@link Expression} parameters and a boolean.
         *
         * @param parameters A List of {@link Expression} parameters that tunes the parsing.
         * @param withFormat A boolean which decides if the parsed BQL String of {@link com.yahoo.bullet.bql.tree.Literal} has format or not.
         */
        public Formatter(Optional<List<Expression>> parameters, boolean withFormat) {
            this.parameters = parameters;
            this.withFormat = withFormat;
        }

        @Override
        protected String visitNode(Node node, Void context) throws UnsupportedOperationException {
            throw new UnsupportedOperationException(format("Not yet implemented: %s.visit%s", getClass().getName(), node.getClass().getSimpleName()));
        }

        @Override
        protected String visitExpression(Expression node, Void context) throws UnsupportedOperationException {
            throw new UnsupportedOperationException(format("Not yet implemented: %s.visit%s", getClass().getName(), node.getClass().getSimpleName()));
        }

        @Override
        protected String visitBooleanLiteral(BooleanLiteral node, Void context) {
            return String.valueOf(node.getValue());
        }

        @Override
        protected String visitStringLiteral(StringLiteral node, Void context) {
            if (!withFormat) {
                return node.getValue();
            }
            return formatStringLiteral(node.getValue());
        }

        @Override
        protected String visitLongLiteral(LongLiteral node, Void context) {
            return Long.toString(node.getValue());
        }

        @Override
        protected String visitDoubleLiteral(DoubleLiteral node, Void context) {
            if (!withFormat) {
                return Double.toString(node.getValue());
            }
            return DOUBLE_FORMATTER.get().format(node.getValue());
        }

        @Override
        protected String visitDecimalLiteral(DecimalLiteral node, Void context) {
            if (!withFormat) {
                return node.getValue();
            }
            return node.getValue();
        }

        @Override
        protected String visitNullLiteral(NullLiteral node, Void context) {
            return "NULL";
        }

        @Override
        protected String visitIdentifier(Identifier node, Void context) {
            if (!withFormat) {
                return node.getValue();
            }
            return node.getValue();
        }

        @Override
        protected String visitDistribution(Distribution node, Void context) {
            return node.attributesToString();
        }

        @Override
        protected String visitTopK(TopK node, Void context) {
            StringBuilder builder = new StringBuilder();
            builder.append("TOP(")
                    .append(node.getSize())
                    .append(", ");
            node.getThreshold().ifPresent(threshold -> builder.append(threshold).append(", "));
            builder.append(process(node.getColumns().get(0)))
                    .append(")");
            return builder.toString();
        }

        @Override
        protected String visitOrderBy(OrderBy node, Void context) {
            return "ORDER BY " +
                    format("%s", Joiner.on(", ").join(node.getSortItems().stream().map(sortItem ->
                            sortItem.getSortKey().toFormatlessString() + (sortItem.getOrdering() == SortItem.Ordering.DESCENDING ? " DESC" : " ASC")
                        ).collect(Collectors.toList())));
        }

        @Override
        protected String visitDereferenceExpression(DereferenceExpression node, Void context) {
            String baseString = process(node.getBase(), context);
            return baseString + "." + process(node.getField());
        }

        @Override
        protected String visitStream(Stream node, Void context) {
            StringBuilder builder = new StringBuilder();
            builder.append("STREAM(")
                    .append(node.getTimeDuration().get())
                    .append(" , TIME");

            if (node.getRecordDuration().isPresent()) {
                builder.append(", ")
                        .append(node.getRecordDuration().get())
                        .append(", RECORD");
            }

            builder.append(")");

            return builder.toString();
        }

        @Override
        protected String visitWindowing(Windowing node, Void context) {
            StringBuilder builder = new StringBuilder();
            builder.append("WINDOWING(EVERY, ")
                    .append(node.getEmitEvery())
                    .append(", ")
                    .append(node.getEmitType().name())
                    .append(", ")
                    .append(process(node.getInclude(), context))
                    .append(")");
            return builder.toString();
        }

        @Override
        protected String visitWindowInclude(WindowInclude node, Void context) {
            StringBuilder builder = new StringBuilder();
            Unit unit = node.getUnit();
            if (unit == ALL) {
                builder.append(unit);
            } else {
                builder.append(node.getType().get().name())
                        .append(", ")
                        .append(node.getNumber().get())
                        .append(", ")
                        .append(unit);
            }

            return builder.toString();
        }

        @Override
        protected String visitFunctionCall(FunctionCall node, Void context) {
            StringBuilder builder = new StringBuilder();

            String arguments = joinExpressions(node.getArguments());
            if (node.getArguments().isEmpty()) {
                arguments = "*";
            }
            if (node.isDistinct()) {
                arguments = "DISTINCT " + arguments;
            }

            builder.append(node.getType())
                    .append('(').append(arguments);

            builder.append(')');

            return builder.toString();
        }

        @Override
        protected String visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context) {
            return formatBinaryExpression(node.getOperation().toString(), node.getLeft(), node.getRight());
        }

        @Override
        protected String visitNotExpression(NotExpression node, Void context) {
            return "(NOT " + process(node.getValue(), context) + ")";
        }

        @Override
        protected String visitComparisonExpression(ComparisonExpression node, Void context) {
            String operation = null;
            switch (node.getOperation()) {
                case EQUALS:
                    operation = "=";
                    break;
                case NOT_EQUALS:
                    operation = "!=";
                    break;
                case LESS_THAN:
                    operation = "<";
                    break;
                case LESS_EQUALS:
                    operation = "<=";
                    break;
                case GREATER_THAN:
                    operation = ">";
                    break;
                case GREATER_EQUALS:
                    operation = ">=";
                    break;
            }

            if (node.isDistinctFrom()) {
                operation = "IS DISTINCT FROM";
            }
            return formatBinaryExpression(operation, node.getLeft(), node.getRight());
        }

        @Override
        protected String visitIsNullPredicate(IsNullPredicate node, Void context) {
            return "(" + process(node.getValue(), context) + " IS NULL)";
        }

        @Override
        protected String visitIsNotNullPredicate(IsNotNullPredicate node, Void context) {
            return "(" + process(node.getValue(), context) + " IS NOT NULL)";
        }

        @Override
        protected String visitArithmeticUnary(ArithmeticUnaryExpression node, Void context) {
            String value = process(node.getValue(), context);
            if (node.getSign() == MINUS) {
                return "-" + value;
            } else {
                return "+" + value;
            }
        }

        @Override
        protected String visitLikePredicate(LikePredicate node, Void context) {
            return "(" + process(node.getValue(), context) + " LIKE " + process(node.getPatterns(), context) + ")";
        }

        @Override
        protected String visitBetweenPredicate(BetweenPredicate node, Void context) {
            return "(" + process(node.getValue(), context) + " BETWEEN " +
                    process(node.getMin(), context) + " AND " + process(node.getMax(), context) + ")";
        }

        @Override
        protected String visitInPredicate(InPredicate node, Void context) {
            return "(" + process(node.getValue(), context) + " IN " + process(node.getValueList(), context) + ")";
        }

        @Override
        protected String visitContainsPredicate(ContainsPredicate node, Void context) {
            String op = null;
            switch (node.getOperation()) {
                case CONTAINS_KEY:
                    op = "CONTAINSKEY";
                    break;
                case CONTAINS_VALUE:
                    op = "CONTAINSVALUE";
                    break;
            }
            return "(" + process(node.getValue(), context) + " " + op + " " + process(node.getValueList(), context) + ")";
        }

        @Override
        protected String visitReferenceWithFunction(ReferenceWithFunction node, Void context) {
            String op = null;
            switch (node.getOperation()) {
                case SIZE_IS:
                    op = "SIZEOF";
                    break;
            }
            return op + "(" + process(node.getValue(), context) + ")";
        }

        @Override
        protected String visitCastExpression(CastExpression node, Void context) {
            return "CAST (" + process(node.getExpression(), context) + ", " + node.getCastType().toUpperCase() + ")";
        }

        @Override
        protected String visitBinaryExpression(InfixExpression node, Void context) {
            return process(node.getLeft(), context) + " " + node.getOp() + " " + process(node.getRight(), context);
        }

        @Override
        protected String visitParensExpression(ParensExpression node, Void context) {
            if (node.getValue() instanceof InfixExpression) {
                return "(" + node.getValue().toFormatlessString() + ")";
            }
            return node.getValue().toFormatlessString();
        }

        @Override
        protected String visitValueListExpression(ValueListExpression node, Void context) {
            return "(" + joinExpressions(node.getValues()) + ")";
        }

        private String formatBinaryExpression(String operator, Expression left, Expression right) {
            return '(' + process(left, null) + ' ' + operator + ' ' + process(right, null) + ')';
        }

        private String joinExpressions(List<Expression> expressions) {
            return Joiner.on(", ").join(expressions.stream()
                    .map((e) -> process(e, null))
                    .iterator());
        }
    }

    static String formatStringLiteral(String s) {
        s = s.replace("'", "''");
        return "'" + s + "'";
    }

    static String formatGroupBy(List<GroupingElement> groupingElements) {
        return formatGroupBy(groupingElements, Optional.empty());
    }

    static String formatGroupBy(List<GroupingElement> groupingElements, Optional<List<Expression>> parameters) {
        ImmutableList.Builder<String> resultStrings = ImmutableList.builder();

        for (GroupingElement groupingElement : groupingElements) {
            String result = "";
            Set<Expression> columns = ImmutableSet.copyOf(((SimpleGroupBy) groupingElement).getColumnExpressions());
            if (columns.size() == 1) {
                result = formatExpression(getOnlyElement(columns), parameters);
            } else {
                result = formatGroupingSet(columns, parameters);
            }
            resultStrings.add(result);
        }
        return Joiner.on(", ").join(resultStrings.build());
    }

    static String formatStream(Stream node) {
        return new Formatter(Optional.empty()).process(node);
    }

    static String formatOrderBy(OrderBy node) {
        return new Formatter(Optional.empty()).process(node);
    }

    static String formatWindowing(Windowing node) {
        return new Formatter(Optional.empty()).process(node);
    }

    private static String formatGroupingSet(Set<Expression> groupingSet, Optional<List<Expression>> parameters) {
        return format("(%s)", Joiner.on(", ").join(groupingSet.stream()
                .map(e -> formatExpression(e, parameters))
                .iterator()));
    }
}
