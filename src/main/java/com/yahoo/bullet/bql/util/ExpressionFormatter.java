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
import com.yahoo.bullet.bql.tree.FieldExpressionNode;
import com.yahoo.bullet.bql.tree.GroupByNode;
import com.yahoo.bullet.bql.tree.GroupOperationNode;
import com.yahoo.bullet.bql.tree.IdentifierNode;
import com.yahoo.bullet.bql.tree.BinaryExpressionNode;
import com.yahoo.bullet.bql.tree.CastExpressionNode;
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
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.bql.tree.SelectNode;
import com.yahoo.bullet.bql.tree.SortItemNode;
import com.yahoo.bullet.bql.tree.StreamNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.bql.tree.UnaryExpressionNode;
import com.yahoo.bullet.bql.tree.WindowIncludeNode;
import com.yahoo.bullet.bql.tree.WindowNode;
import com.yahoo.bullet.parsing.Window;
import lombok.AllArgsConstructor;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

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
            throw new UnsupportedOperationException(String.format("Not yet implemented: %s.visit%s", getClass().getName(), node.getClass().getSimpleName()));
        }

        @Override
        protected String visitExpression(ExpressionNode node, Void context) throws UnsupportedOperationException {
            throw new UnsupportedOperationException(String.format("Not yet implemented: %s.visit%s", getClass().getName(), node.getClass().getSimpleName()));
        }

        @Override
        protected String visitQuery(QueryNode node, Void context) {
            StringBuilder builder = new StringBuilder();
            builder.append(process(node.getSelect()))
                   .append(" FROM ")
                   .append(process(node.getStream()));
            if (node.getWhere() != null) {
                builder.append(" WHERE ")
                       .append(process(node.getWhere()));
            }
            if (node.getGroupBy() != null) {
                builder.append(" ")
                       .append(process(node.getGroupBy()));
            }
            if (node.getHaving() != null) {
                builder.append(" HAVING ")
                       .append(process(node.getHaving()));
            }
            if (node.getOrderBy() != null) {
                builder.append(" ")
                       .append(process(node.getOrderBy()));
            }
            if (node.getWindow() != null) {
                builder.append(" ")
                       .append(process(node.getWindow()));
            }
            if (node.getLimit() != null) {
                builder.append(" LIMIT ")
                       .append(node.getLimit());
            }
            return builder.toString();
        }

        @Override
        protected String visitSelect(SelectNode node, Void context) {
            return "SELECT " + join(node.getSelectItems());
        }

        @Override
        protected String visitSelectItem(SelectItemNode node, Void context) {
            if (node.isAll()) {
                return "*";
            }
            return process(node.getExpression()) + (node.getAlias() != null ? " AS " + process(node.getAlias()) : "");
        }

        @Override
        protected String visitStream(StreamNode node, Void context) {
            if (node.getTimeDuration() == null) {
                return "STREAM()";
            }
            if (node.getRecordDuration() == null) {
                return "STREAM(" + node.getTimeDuration() + ", TIME)";
            }
            return "STREAM(" + node.getTimeDuration() + ", TIME, " + node.getRecordDuration() + ", RECORD)";
        }

        @Override
        protected String visitGroupBy(GroupByNode node, Void context) {
            return "GROUP BY " + join(node.getExpressions());
        }

        @Override
        protected String visitOrderBy(OrderByNode node, Void context) {
            return "ORDER BY " + join(node.getSortItems());
        }

        @Override
        protected String visitSortItem(SortItemNode node, Void context) {
            return process(node.getExpression()) + " " + (node.getOrdering() == SortItemNode.Ordering.DESCENDING ? "DESC" : "ASC");
        }

        @Override
        protected String visitWindow(WindowNode node, Void context) {
            if (node.getWindowInclude() != null) {
                return "WINDOWING EVERY(" + node.getEmitEvery() + ", " + node.getEmitType() + ", " + process(node.getWindowInclude()) + ")";
            }
            return "WINDOWING TUMBLING(" + node.getEmitEvery() + ", " + node.getEmitType() + ")";
        }

        @Override
        protected String visitWindowInclude(WindowIncludeNode node, Void context) {
            if (node.getUnit() == Window.Unit.ALL) {
                return node.getUnit().toString();
            }
            return "FIRST, " + node.getNumber() + ", " + node.getUnit();
        }

        @Override
        protected String visitFieldExpression(FieldExpressionNode node, Void context) {
            return process(node.getField()) +
                   (node.getIndex() != null ? "[" + node.getIndex() + "]" : "") +
                   (node.getKey() != null ? "." + process(node.getKey()) : "") +
                   (node.getSubKey() != null ? "." + process(node.getSubKey()) : "");
        }

        @Override
        protected String visitListExpression(ListExpressionNode node, Void context) {
            return "[" + join(node.getExpressions()) + "]";
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
            return node.getOp().getName() + "(" + join(node.getExpressions()) + ")";
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
            return "COUNT(DISTINCT " + join(node.getExpressions()) + ")";
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
            builder.append(join(node.getExpressions()))
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
            if (withFormat && node.isQuoted()) {
                return "\"" + node.getValue() + "\"";
            }
            return node.getValue();
        }

        @Override
        protected String visitLiteral(LiteralNode node, Void context) {
            Object value = node.getValue();
            if (value == null) {
                return "NULL";
            }
            // TODO delete?
            if (withFormat) {
                if (value instanceof Double || value instanceof Float) {
                    return DOUBLE_FORMATTER.get().format(value);
                } else if (value instanceof String) {
                    return formatStringLiteral((String) value);
                }
            }
            if (value instanceof String) {
                return "'" + value + "'";
            }
            return value.toString();
        }

        private String join(List list) {
            return Joiner.on(", ").join(list.stream().map(node -> process((Node) node)).iterator());
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
    public static String format(Node node) {
        return new Formatter(true).process(node);
    }

    static String formatStream(StreamNode node) {
        return new Formatter(true).process(node);
    }

    static String formatOrderBy(OrderByNode node) {
        return new Formatter(true).process(node);
    }

    static String formatWindowing(WindowNode node) {
        return new Formatter(true).process(node);
    }

    //private static String formatGroupingSet(Set<ExpressionNode> groupingSet) {
    //    return format("(%s)", Joiner.on(", ").join(groupingSet.stream().map(ExpressionFormatter::formatExpression).iterator()));
    //}
}
