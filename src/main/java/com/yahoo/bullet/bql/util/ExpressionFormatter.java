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
import com.yahoo.bullet.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.bql.tree.ASTVisitor;
import com.yahoo.bullet.bql.tree.CountDistinctNode;
import com.yahoo.bullet.bql.tree.DistributionNode;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.GroupOperationNode;
import com.yahoo.bullet.bql.tree.IdentifierNode;
import com.yahoo.bullet.bql.tree.BinaryExpressionNode;
import com.yahoo.bullet.bql.tree.CastExpressionNode;
import com.yahoo.bullet.bql.tree.ListExpressionNode;
import com.yahoo.bullet.bql.tree.LiteralNode;
import com.yahoo.bullet.bql.tree.NAryExpressionNode;
import com.yahoo.bullet.bql.tree.Node;
import com.yahoo.bullet.bql.tree.NullPredicateNode;
import com.yahoo.bullet.bql.tree.OrderByNode;
import com.yahoo.bullet.bql.tree.ParenthesesExpressionNode;
import com.yahoo.bullet.bql.tree.StreamNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.bql.tree.UnaryExpressionNode;
import com.yahoo.bullet.bql.tree.WindowNode;
import lombok.AllArgsConstructor;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static java.lang.String.format;

public final class ExpressionFormatter {
    private static final ThreadLocal<DecimalFormat> DOUBLE_FORMATTER = ThreadLocal.withInitial(
            () -> new DecimalFormat("0.###################E0###", new DecimalFormatSymbols(Locale.US)));

    /**
     * Parse an {@link ExpressionNode} to a formatted BQL String, with a List of {@link ExpressionNode} parameters.
     *
     * @param expression A non-null {@link ExpressionNode} will be parsed.
     * @return A formatted BQL String represents the passed in {@link ExpressionNode}.
     */
    public static String formatExpression(ExpressionNode expression){
        return formatExpression(expression, true);
    }

    /**
     * Parse an {@link ExpressionNode} to a BQL String, with a List of {@link ExpressionNode} parameters.
     * For a literal node, the BQL String can be generated with or without format.
     *
     * @param expression A non-null {@link ExpressionNode} will be parsed.
     * @param withFormat A boolean which decides if the parsed BQL String of a literal node has format or not.
     * @return A BQL String represents the passed in {@link ExpressionNode}.
     */
    public static String formatExpression(ExpressionNode expression, boolean withFormat) {
        return new Formatter(withFormat).process(expression);
    }

    @AllArgsConstructor
    public static class Formatter extends ASTVisitor<String, Void> {
        private boolean withFormat;

        @Override
        protected String visitNode(Node node, Void context) throws UnsupportedOperationException {
            throw new UnsupportedOperationException(format("Not yet implemented: %s.visit%s", getClass().getName(), node.getClass().getSimpleName()));
        }

        @Override
        protected String visitExpression(ExpressionNode node, Void context) throws UnsupportedOperationException {
            throw new UnsupportedOperationException(format("Not yet implemented: %s.visit%s", getClass().getName(), node.getClass().getSimpleName()));
        }

        @Override
        protected String visitListExpression(ListExpressionNode node, Void context) {
            return "[" + joinExpressions(node.getExpressions()) + "]";
        }

        @Override
        protected String visitNullPredicate(NullPredicateNode node, Void context) {
            return process(node.getExpression()) + (node.isNot() ? " IS NOT NULL" : " IS NULL");
        }

        @Override
        protected String visitUnaryExpression(UnaryExpressionNode node, Void context) {
            return node.getOp().getName() + " " + process(node.getExpression());
        }

        @Override
        protected String visitNAryExpression(NAryExpressionNode node, Void context) {
            return node.getOp().getName() + "(" + joinExpressions(node.getExpressions()) + ")";
        }

        @Override
        protected String visitGroupOperation(GroupOperationNode node, Void context) {
            if (node.getOp() == GroupOperation.GroupOperationType.COUNT) {
                return "COUNT(*)";
            }
            return node.getOp().getName() + "(" + process(node.getExpression()) + ")";
        }

        @Override
        protected String visitCountDistinct(CountDistinctNode node, Void context) {
            return "COUNT(DISTINCT " + joinExpressions(node.getExpressions()) + ")";
        }

        @Override
        protected String visitDistribution(DistributionNode node, Void context) {
            return node.attributesToString();
        }

        @Override
        protected String visitTopK(TopKNode node, Void context) {
            StringBuilder builder = new StringBuilder();
            builder.append("TOP(")
                   .append(node.getSize())
                   .append(", ");
            if (node.getThreshold() != null) {
                builder.append(node.getThreshold())
                       .append(", ");
            }
            builder.append(joinExpressions(node.getExpressions()))
                   .append(")");
            return builder.toString();
        }

        @Override
        protected String visitCastExpression(CastExpressionNode node, Void context) {
            return "CAST (" + process(node.getExpression()) + " AS " + node.getCastType() + ")";
        }

        @Override
        protected String visitBinaryExpression(BinaryExpressionNode node, Void context) {
            if (node.getOp().isInfix()) {
                return process(node.getLeft()) + " " + node.getOp() + " " + process(node.getRight());
            }
            return node.getOp() + "(" + process(node.getLeft()) + ", " + process(node.getRight());
        }

        @Override
        protected String visitParenthesesExpression(ParenthesesExpressionNode node, Void context) {
            return "(" + process(node.getExpression()) + ")";
        }

        @Override
        protected String visitIdentifier(IdentifierNode node, Void context) {
            return node.getValue();
        }

        @Override
        protected String visitLiteral(LiteralNode node, Void context) {
            Object value = node.getValue();
            if (value == null) {
                return "NULL";
            }
            if (withFormat) {
                if (value instanceof Double || value instanceof Float) {
                    return DOUBLE_FORMATTER.get().format(value);
                } else if (value instanceof String) {
                    return formatStringLiteral((String) value);
                }
            }
            return value.toString();
        }

        /*
        @Override
        protected String visitOrderBy(OrderByNode node, Void context) {
            return "ORDER BY " +
                    format("%s", Joiner.on(", ").join(node.getSortItems().stream().map(sortItem ->
                            sortItem.getSortKey().toFormatlessString() + (sortItem.getOrdering() == SortItemNode.Ordering.DESCENDING ? " DESC" : " ASC")
                        ).collect(Collectors.toList())));
        }

        @Override
        protected String visitStream(StreamNode node, Void context) {
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
        protected String visitWindow(WindowNode node, Void context) {
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
        protected String visitWindowInclude(WindowIncludeNode node, Void context) {
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
        */

        private String joinExpressions(List<ExpressionNode> expressions) {
            return Joiner.on(", ").join(expressions.stream().map(this::process).iterator());
        }
    }

    private static String formatStringLiteral(String s) {
        s = s.replace("'", "''");
        return "'" + s + "'";
    }
    /*
    static String formatGroupBy(List<GroupingElement> groupingElements) {
        return formatGroupBy(groupingElements, Optional.empty());
    }

    static String formatGroupBy(List<GroupingElement> groupingElements, Optional<List<ExpressionNode>> parameters) {
        ImmutableList.Builder<String> resultStrings = ImmutableList.builder();

        for (GroupingElement groupingElement : groupingElements) {
            String result = "";
            Set<ExpressionNode> columns = ImmutableSet.copyOf(((SimpleGroupBy) groupingElement).getColumnExpressions());
            if (columns.size() == 1) {
                result = formatExpression(getOnlyElement(columns));
            } else {
                result = formatGroupingSet(columns);
            }
            resultStrings.add(result);
        }
        return Joiner.on(", ").join(resultStrings.build());
    }
    */
    static String formatStream(StreamNode node) {
        return new Formatter(true).process(node);
    }

    static String formatOrderBy(OrderByNode node) {
        return new Formatter(true).process(node);
    }

    static String formatWindowing(WindowNode node) {
        return new Formatter(true).process(node);
    }

    private static String formatGroupingSet(Set<ExpressionNode> groupingSet) {
        return format("(%s)", Joiner.on(", ").join(groupingSet.stream().map(ExpressionFormatter::formatExpression).iterator()));
    }
}
