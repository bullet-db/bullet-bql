/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/SqlFormatter.java
 */
package com.yahoo.bullet.bql.util;

import com.google.common.base.Strings;
import com.yahoo.bullet.bql.tree.ASTVisitor;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.Node;
import com.yahoo.bullet.bql.tree.SelectNode;
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.bql.tree.StreamNode;
import com.yahoo.bullet.bql.tree.WindowNode;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.yahoo.bullet.bql.util.ExpressionFormatter.formatExpression;
import static com.yahoo.bullet.bql.util.ExpressionFormatter.formatStream;
import static com.yahoo.bullet.bql.util.ExpressionFormatter.formatWindowing;

public final class BQLFormatter {
    private static final String INDENT = "   ";

    private BQLFormatter() {
    }

    /**
     * Parse a {@link Node} tree to a formatted BQL String. This is used to check if two {@link Node} tree are same.
     *
     * @param root The root of the {@link Node} tree
     * @return A formatted string representation of BQL statement
     */
    public static String formatBQL(Node root) {
        StringBuilder builder = new StringBuilder();
        new Formatter(builder).process(root, 0);
        return builder.toString();
    }

    private static class Formatter extends ASTVisitor<Void, Integer> {
        private final StringBuilder builder;

        public Formatter(StringBuilder builder) {
            this.builder = builder;
        }

        @Override
        protected Void visitNode(Node node, Integer indent) {
            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        @Override
        protected Void visitExpression(ExpressionNode node, Integer indent) {
            checkArgument(indent == 0, "visitExpression should only be called at root");
            builder.append(formatExpression(node));
            return null;
        }

        /*@Override
        protected Void visitQuery(Query node, Integer indent) {
            processRelation(node.getQueryBody(), indent);

            if (node.getOrderBy().isPresent()) {
                process(node.getOrderBy().get(), indent);
            }

            if (node.getLimit().isPresent()) {
                append(indent, "LIMIT " + node.getLimit().get())
                        .append('\n');
            }

            return null;
        }

        @Override
        protected Void visitQuerySpecification(QuerySpecification node, Integer indent) {
            process(node.getSelect(), indent);

            if (node.getFrom().isPresent()) {
                append(indent, "FROM ");
                process(node.getFrom().get(), indent);
                builder.append('\n');
            }

            if (node.getWhere().isPresent()) {
                append(indent, "WHERE " + formatExpression(node.getWhere().get(), parameters))
                        .append('\n');
            }

            if (node.getGroupBy().isPresent()) {
                append(indent, "GROUP BY " + (node.getGroupBy().get().isDistinct() ? " DISTINCT " : "") + ExpressionFormatter.formatGroupBy(node.getGroupBy().get().getGroupingElements())).append('\n');
            }

            if (node.getOrderBy().isPresent()) {
                append(indent, formatOrderBy(node.getOrderBy().get()) + '\n');
            }

            if (node.getWindowing().isPresent()) {
                process(node.getWindowing().get(), indent);
                builder.append('\n');
            }

            if (node.getLimit().isPresent()) {
                append(indent, "LIMIT " + node.getLimit().get())
                        .append('\n');
            }
            return null;
        }*/

        @Override
        protected Void visitSelect(SelectNode node, Integer indent) {
            append(indent, "SELECT");
            if (node.isDistinct()) {
                builder.append(" DISTINCT");
            }

            if (node.getSelectItems().size() > 1) {
                boolean first = true;
                for (SelectItemNode item : node.getSelectItems()) {
                    builder.append("\n")
                            .append(indentString(indent))
                            .append(first ? "  " : ", ");

                    process(item, indent);
                    first = false;
                }
            } else {
                builder.append(' ');
                process(getOnlyElement(node.getSelectItems()), indent);
            }

            builder.append('\n');

            return null;
        }

        @Override
        protected Void visitWindow(WindowNode node, Integer indent) {
            builder.append(formatWindowing(node));

            return null;
        }

        @Override
        protected Void visitStream(StreamNode node, Integer indent) {
            builder.append(formatStream(node));

            return null;
        }
        private StringBuilder append(int indent, String value) {
            return builder.append(indentString(indent))
                    .append(value);
        }

        private static String indentString(int indent) {
            return Strings.repeat(INDENT, indent);
        }
    }

    /*private static void appendAliasColumns(StringBuilder builder, List<IdentifierNode> columns) {
        if ((columns != null) && (!columns.isEmpty())) {
            String formattedColumns = columns.stream()
                    .map(ExpressionFormatter::formatExpression))
                    .collect(Collectors.joining(", "));

            builder.append(" (")
                    .append(formattedColumns)
                    .append(')');
        }
    }*/
}
