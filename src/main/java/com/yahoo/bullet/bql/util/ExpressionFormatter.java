/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/ExpressionFormatter.java
 */
package com.yahoo.bullet.bql.util;

import com.yahoo.bullet.bql.tree.ASTVisitor;
import com.yahoo.bullet.bql.tree.BetweenPredicateNode;
import com.yahoo.bullet.bql.tree.CountDistinctNode;
import com.yahoo.bullet.bql.tree.DistributionNode;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.FieldExpressionNode;
import com.yahoo.bullet.bql.tree.GroupByNode;
import com.yahoo.bullet.bql.tree.GroupOperationNode;
import com.yahoo.bullet.bql.tree.IdentifierNode;
import com.yahoo.bullet.bql.tree.BinaryExpressionNode;
import com.yahoo.bullet.bql.tree.CastExpressionNode;
import com.yahoo.bullet.bql.tree.LateralViewNode;
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
import com.yahoo.bullet.bql.tree.SubFieldExpressionNode;
import com.yahoo.bullet.bql.tree.TableFunctionNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.bql.tree.UnaryExpressionNode;
import com.yahoo.bullet.bql.tree.WindowIncludeNode;
import com.yahoo.bullet.bql.tree.WindowNode;
import com.yahoo.bullet.query.Window;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import lombok.AllArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

public final class ExpressionFormatter {
    @AllArgsConstructor
    public static class Formatter extends ASTVisitor<String, Void> {
        private static final String DELIMITER = ", ";
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
            if (node.getOuterQuery() == null) {
                return visitQueryNode(node, null);
            } else {
                return visitQueryNode(node.getOuterQuery(), node);
            }
        }

        private String visitQueryNode(QueryNode queryNode, QueryNode innerQueryNode) {
            StringBuilder builder = new StringBuilder();
            builder.append(process(queryNode.getSelect()))
                   .append(" FROM ");
            if (innerQueryNode != null) {
                builder.append("(")
                       .append(visitQueryNode(innerQueryNode, null))
                       .append(")");
            } else {
                builder.append(process(queryNode.getStream()));
            }
            if (queryNode.getLateralView() != null) {
                builder.append(" ")
                       .append(process(queryNode.getLateralView()));
            }
            if (queryNode.getWhere() != null) {
                builder.append(" WHERE ")
                       .append(process(queryNode.getWhere()));
            }
            if (queryNode.getGroupBy() != null) {
                builder.append(" ")
                       .append(process(queryNode.getGroupBy()));
            }
            if (queryNode.getHaving() != null) {
                builder.append(" HAVING ")
                       .append(process(queryNode.getHaving()));
            }
            if (queryNode.getOrderBy() != null) {
                builder.append(" ")
                       .append(process(queryNode.getOrderBy()));
            }
            if (queryNode.getWindow() != null) {
                builder.append(" ")
                       .append(process(queryNode.getWindow()));
            }
            if (queryNode.getLimit() != null) {
                builder.append(" LIMIT ")
                       .append(queryNode.getLimit());
            }
            return builder.toString();
        }

        @Override
        protected String visitSelect(SelectNode node, Void context) {
            return "SELECT " + (node.isDistinct() ? "DISTINCT " : "") + join(node.getSelectItems());
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
            return "STREAM(" + node.getTimeDuration() + ", TIME)";
        }

        @Override
        protected String visitLateralView(LateralViewNode node, Void context) {
            return node.getTableFunctions().stream().map(tableFunctionNode -> "LATERAL VIEW " + process(tableFunctionNode)).collect(Collectors.joining(" "));
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
            if (node.getIncludeUnit() == Window.Unit.ALL) {
                return node.getIncludeUnit().toString();
            }
            return "FIRST, " + node.getFirst() + ", " + node.getIncludeUnit();
        }

        @Override
        protected String visitFieldExpression(FieldExpressionNode node, Void context) {
            return process(node.getField());
        }

        @Override
        protected String visitSubFieldExpression(SubFieldExpressionNode node, Void context) {
            if (node.getIndex() != null) {
                return process(node.getField()) + "[" + node.getIndex() + "]";
            } else if (node.getKey() != null) {
                return process(node.getField()) + "." + process(node.getKey());
            } else if (node.getExpressionKey() != null) {
                return process(node.getField()) + "[" + process(node.getExpressionKey()) + "]";
            } else {
                return process(node.getField()) + "['" + node.getStringKey() + "']";
            }
        }

        @Override
        protected String visitListExpression(ListExpressionNode node, Void context) {
            return node.isParenthesized() ? "(" + join(node.getExpressions()) + ")" : "[" + join(node.getExpressions()) + "]";
        }

        @Override
        protected String visitNullPredicate(NullPredicateNode node, Void context) {
            return process(node.getExpression()) + (node.isNot() ? " IS NOT NULL" : " IS NULL");
        }

        @Override
        protected String visitBetweenPredicate(BetweenPredicateNode node, Void context) {
            if (node.isNot()) {
                return process(node.getExpression()) + " NOT BETWEEN (" + process(node.getLower()) + ", " + process(node.getUpper()) + ")";
            }
            return process(node.getExpression()) + " BETWEEN (" + process(node.getLower()) + ", " + process(node.getUpper()) + ")";
        }

        @Override
        protected String visitUnaryExpression(UnaryExpressionNode node, Void context) {
            if (node.isParenthesized()) {
                return node.getOp() + "(" + process(node.getExpression()) + ")";
            }
            return node.getOp() + " " + process(node.getExpression());
        }

        @Override
        protected String visitNAryExpression(NAryExpressionNode node, Void context) {
            return node.getOp() + "(" + join(node.getExpressions()) + ")";
        }

        @Override
        protected String visitGroupOperation(GroupOperationNode node, Void context) {
            if (node.getOp() == GroupOperation.GroupOperationType.COUNT) {
                return "COUNT(*)";
            }
            return node.getOp() + "(" + process(node.getExpression()) + ")";
        }

        @Override
        protected String visitCountDistinct(CountDistinctNode node, Void context) {
            return "COUNT(DISTINCT " + join(node.getExpressions()) + ")";
        }

        @Override
        protected String visitDistribution(DistributionNode node, Void context) {
            return node.toString();
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
            return "CAST(" + process(node.getExpression()) + " AS " + node.getCastType() + ")";
        }

        @Override
        protected String visitTableFunction(TableFunctionNode node, Void context) {
            StringBuilder builder = new StringBuilder();
            if (node.isOuter()) {
                builder.append("OUTER ");
            }
            switch (node.getType()) {
                case EXPLODE:
                    builder.append("EXPLODE(");
                    break;
                default:
                    builder.append(node.getType())
                           .append("(");
            }
            builder.append(process(node.getExpression()))
                   .append(") AS ");
            if (node.getValueAlias() != null) {
                builder.append("(")
                       .append(process(node.getKeyAlias()))
                       .append(", ")
                       .append(process(node.getValueAlias()))
                       .append(")");
            } else {
                builder.append(process(node.getKeyAlias()));
            }
            return builder.toString();
        }

        @Override
        protected String visitBinaryExpression(BinaryExpressionNode node, Void context) {
            switch (node.getOp()) {
                case ADD:
                case SUB:
                case MUL:
                case DIV:
                case MOD:
                case EQUALS:
                case EQUALS_ANY:
                case EQUALS_ALL:
                case NOT_EQUALS:
                case NOT_EQUALS_ANY:
                case NOT_EQUALS_ALL:
                case GREATER_THAN:
                case GREATER_THAN_ANY:
                case GREATER_THAN_ALL:
                case LESS_THAN:
                case LESS_THAN_ANY:
                case LESS_THAN_ALL:
                case GREATER_THAN_OR_EQUALS:
                case GREATER_THAN_OR_EQUALS_ANY:
                case GREATER_THAN_OR_EQUALS_ALL:
                case LESS_THAN_OR_EQUALS:
                case LESS_THAN_OR_EQUALS_ANY:
                case LESS_THAN_OR_EQUALS_ALL:
                case REGEX_LIKE:
                case REGEX_LIKE_ANY:
                case NOT_REGEX_LIKE:
                case NOT_REGEX_LIKE_ANY:
                case IN:
                case NOT_IN:
                case AND:
                case OR:
                case XOR:
                    return process(node.getLeft()) + " " + node.getOp() + " " + process(node.getRight());
                default:
                    return node.getOp() + "(" + process(node.getLeft()) + ", " + process(node.getRight()) + ")";
            }
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
            Serializable value = node.getValue();
            if (value == null) {
                return "NULL";
            }
            if (value instanceof String) {
                return formatStringLiteral((String) value, withFormat);
            } else if (value instanceof Long) {
                return value + "L";
            } else if (value instanceof Float) {
                return value + "f";
            }
            return value.toString();
        }

        private <T extends Node> String join(List<T> list) {
            return list.stream().map(this::process).collect(Collectors.joining(DELIMITER));
        }

        private static String formatStringLiteral(String s, boolean withFormat) {
            if (withFormat) {
                s = s.replace("'", "''");
            }
            return "'" + s + "'";
        }
    }

    /**
     * Formats the given {@link Node} as a {@link String}.
     *
     * @param node The {@link Node} to format.
     * @param withFormat A boolean which decides if the parsed BQL String of a literal node has format or not.
     * @return The string representation of the given {@link Node}.
     */
    public static String format(Node node, boolean withFormat) {
        return new Formatter(withFormat).process(node);
    }
}
