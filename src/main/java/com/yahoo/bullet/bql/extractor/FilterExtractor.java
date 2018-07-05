/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.tree.ASTVisitor;
import com.yahoo.bullet.bql.tree.BetweenPredicate;
import com.yahoo.bullet.bql.tree.ComparisonExpression;
import com.yahoo.bullet.bql.tree.Expression;
import com.yahoo.bullet.bql.tree.InPredicate;
import com.yahoo.bullet.bql.tree.IsEmptyPredicate;
import com.yahoo.bullet.bql.tree.IsNotEmptyPredicate;
import com.yahoo.bullet.bql.tree.IsNotNullPredicate;
import com.yahoo.bullet.bql.tree.IsNullPredicate;
import com.yahoo.bullet.bql.tree.LikePredicate;
import com.yahoo.bullet.bql.tree.LogicalBinaryExpression;
import com.yahoo.bullet.bql.tree.Node;
import com.yahoo.bullet.bql.tree.NotExpression;
import com.yahoo.bullet.bql.tree.Query;
import com.yahoo.bullet.bql.tree.QuerySpecification;
import com.yahoo.bullet.parsing.Clause;
import com.yahoo.bullet.parsing.FilterClause;
import com.yahoo.bullet.parsing.LogicalClause;

import java.util.List;

import static com.yahoo.bullet.parsing.Clause.Operation;
import static com.yahoo.bullet.parsing.Clause.Operation.AND;
import static com.yahoo.bullet.parsing.Clause.Operation.EQUALS;
import static com.yahoo.bullet.parsing.Clause.Operation.GREATER_EQUALS;
import static com.yahoo.bullet.parsing.Clause.Operation.LESS_EQUALS;
import static com.yahoo.bullet.parsing.Clause.Operation.NOT;
import static com.yahoo.bullet.parsing.Clause.Operation.NOT_EQUALS;
import static com.yahoo.bullet.parsing.Clause.Operation.REGEX_LIKE;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class FilterExtractor {
    private static final ExtractVisitor FILTER_VISITOR = new ExtractVisitor();

    /**
     * Extract a List of {@link Clause} from a {@link Node} tree. Currently this is used to extract a WHERE clause from BQL.
     *
     * @param node The non-null root of the {@link Node} tree.
     * @return A List of {@link Clause}.
     * @throws NullPointerException when node is null.
     */
    public List<Clause> extractFilter(Node node) throws NullPointerException {
        requireNonNull(node);

        return asList(FILTER_VISITOR.process(node));
    }

    private static class ExtractVisitor extends ASTVisitor<Clause, Void> {
        @Override
        protected Clause visitQuery(Query node, Void context) {
            return process(node.getQueryBody());
        }

        @Override
        protected Clause visitQuerySpecification(QuerySpecification node, Void context) {
            if (!node.getWhere().isPresent()) {
                return null;
            }
            return process(node.getWhere().get());
        }

        @Override
        protected Clause visitNotExpression(NotExpression node, Void context) {
            LogicalClause notClause = new LogicalClause();
            notClause.setOperation(NOT);
            notClause.setClauses(asList(process(node.getValue())));
            return notClause;
        }

        @Override
        protected Clause visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context) {
            LogicalClause binaryClause = new LogicalClause();
            binaryClause.setOperation(node.getOperation());
            binaryClause.setClauses(asList(process(node.getLeft()), process(node.getRight())));
            return binaryClause;
        }

        @Override
        protected Clause visitComparisonExpression(ComparisonExpression node, Void context) {
            return createFilterClause(node.getOperation(), node.getLeft().toFormatlessString(), node.getRight().toFormatlessString());
        }

        // BetweenPredicate generates a AND clause that contains two comparison clauses.
        @Override
        protected Clause visitBetweenPredicate(BetweenPredicate node, Void context) {
            LogicalClause binaryClause = new LogicalClause();
            binaryClause.setOperation(AND);

            Clause lowerClause = createFilterClause(GREATER_EQUALS, node.getValue().toFormatlessString(), node.getMin().toFormatlessString());
            Clause upperClause = createFilterClause(LESS_EQUALS, node.getValue().toFormatlessString(), node.getMax().toFormatlessString());

            binaryClause.setClauses(asList(lowerClause, upperClause));

            return binaryClause;
        }

        @Override
        protected Clause visitInPredicate(InPredicate node, Void context) {
            List<Expression> inList = node.getInList();
            String[] values = inList.stream().map(Expression::toFormatlessString).toArray(String[]::new);
            return createFilterClause(EQUALS, node.getValue().toFormatlessString(), values);
        }

        @Override
        protected Clause visitLikePredicate(LikePredicate node, Void context) {
            List<Expression> likeList = node.getLikeList();
            String[] values = likeList.stream().map(Expression::toFormatlessString).toArray(String[]::new);
            return createFilterClause(REGEX_LIKE, node.getValue().toFormatlessString(), values);
        }

        @Override
        protected Clause visitIsNullPredicate(IsNullPredicate node, Void context) {
            return createFilterClause(EQUALS, node.getValue().toFormatlessString(), "NULL");
        }

        @Override
        protected Clause visitIsNotNullPredicate(IsNotNullPredicate node, Void context) {
            return createFilterClause(NOT_EQUALS, node.getValue().toFormatlessString(), "NULL");
        }

        @Override
        protected Clause visitIsEmptyPredicate(IsEmptyPredicate node, Void context) {
            return createFilterClause(EQUALS, node.getValue().toFormatlessString(), "");
        }

        @Override
        protected Clause visitIsNotEmptyPredicate(IsNotEmptyPredicate node, Void context) {
            return createFilterClause(NOT_EQUALS, node.getValue().toFormatlessString(), "");
        }

        private Clause createFilterClause(Operation operation, String field, String... values) {
            FilterClause filterClause = new FilterClause();
            filterClause.setOperation(operation);
            filterClause.setField(field);
            filterClause.setValues(asList(values));

            return filterClause;
        }
    }
}
