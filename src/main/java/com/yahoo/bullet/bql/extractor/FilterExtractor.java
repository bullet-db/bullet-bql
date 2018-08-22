/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.tree.ASTVisitor;
import com.yahoo.bullet.bql.tree.BetweenPredicate;
import com.yahoo.bullet.bql.tree.ComparisonExpression;
import com.yahoo.bullet.bql.tree.Expression;
import com.yahoo.bullet.bql.tree.ContainsPredicate;
import com.yahoo.bullet.bql.tree.Identifier;
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
import com.yahoo.bullet.bql.tree.ReferenceWithFunction;
import com.yahoo.bullet.parsing.Clause;
import com.yahoo.bullet.parsing.FilterClause;
import com.yahoo.bullet.parsing.LogicalClause;
import com.yahoo.bullet.parsing.ObjectFilterClause;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
            Expression left = node.getLeft();
            Operation op = node.getOperation();
            boolean isNotExpression = false;
            if (left instanceof ReferenceWithFunction) {
                if (op != EQUALS && op != NOT_EQUALS) {
                    throw new ParsingException("Only '==', '!=', '<>', 'DISTINCT FROM' or 'IN' are supported in " + ((ReferenceWithFunction) left).getOperation());
                }
                if (op == NOT_EQUALS) {
                    isNotExpression = true;
                }
                op = ((ReferenceWithFunction) left).getOperation();
                left = ((ReferenceWithFunction) left).getValue();
            }
            Clause clause = createFilterClause(op, left.toFormatlessString(), node.getRight());
            if (!isNotExpression) {
                return clause;
            }

            LogicalClause notClause = new LogicalClause();
            notClause.setOperation(NOT);
            notClause.setClauses(asList(clause));
            return notClause;
        }

        // BetweenPredicate generates a AND clause that contains two comparison clauses.
        @Override
        protected Clause visitBetweenPredicate(BetweenPredicate node, Void context) {
            Expression value = node.getValue();
            if (value instanceof ReferenceWithFunction) {
                throw new ParsingException("Only '==', '!=', '<>', 'DISTINCT FROM' or 'IN' are supported in " + ((ReferenceWithFunction) value).getOperation());
            }

            LogicalClause binaryClause = new LogicalClause();
            binaryClause.setOperation(AND);

            Clause lowerClause = createFilterClause(GREATER_EQUALS, value.toFormatlessString(), node.getMin());
            Clause upperClause = createFilterClause(LESS_EQUALS, value.toFormatlessString(), node.getMax());

            binaryClause.setClauses(asList(lowerClause, upperClause));

            return binaryClause;
        }

        @Override
        protected Clause visitInPredicate(InPredicate node, Void context) {
            Expression value = node.getValue();
            Operation op = EQUALS;
            if (value instanceof ReferenceWithFunction) {
                op = ((ReferenceWithFunction) value).getOperation();
                value = ((ReferenceWithFunction) value).getValue();
            }

            List<Expression> inList = node.getInList();
            return createFilterClause(op, value.toFormatlessString(), inList.toArray(new Expression[0]));
        }

        @Override
        protected Clause visitContainsPredicate(ContainsPredicate node, Void context) {
            Expression value = node.getValue();
            if (value instanceof ReferenceWithFunction) {
                throw new ParsingException("Only '==', '!=', '<>', 'DISTINCT FROM' or 'IN' are supported in " + ((ReferenceWithFunction) value).getOperation());
            }

            List<Expression> valueList = node.getContainsList();
            return createFilterClause(node.getOperation(), value.toFormatlessString(), valueList.toArray(new Expression[0]));
        }

        @Override
        protected Clause visitLikePredicate(LikePredicate node, Void context) {
            Expression value = node.getValue();
            if (value instanceof ReferenceWithFunction) {
                throw new ParsingException("Only '==', '!=', '<>', 'DISTINCT FROM' or 'IN' are supported in " + ((ReferenceWithFunction) value).getOperation());
            }

            List<Expression> likeList = node.getLikeList();
            return createFilterClause(REGEX_LIKE, value.toFormatlessString(), likeList.toArray(new Expression[0]));
        }

        @Override
        protected Clause visitIsNullPredicate(IsNullPredicate node, Void context) {
            Expression value = node.getValue();
            if (value instanceof ReferenceWithFunction) {
                throw new ParsingException("Only '==', '!=', '<>', 'DISTINCT FROM' or 'IN' are supported in " + ((ReferenceWithFunction) value).getOperation());
            }

            return createFilterClause(EQUALS, value.toFormatlessString(), "NULL");
        }

        @Override
        protected Clause visitIsNotNullPredicate(IsNotNullPredicate node, Void context) {
            Expression value = node.getValue();
            if (value instanceof ReferenceWithFunction) {
                throw new ParsingException("Only '==', '!=', '<>', 'DISTINCT FROM' or 'IN' are supported in " + ((ReferenceWithFunction) value).getOperation());
            }
            return createFilterClause(NOT_EQUALS, value.toFormatlessString(), "NULL");
        }

        @Override
        protected Clause visitIsEmptyPredicate(IsEmptyPredicate node, Void context) {
            Expression value = node.getValue();
            if (value instanceof ReferenceWithFunction) {
                throw new ParsingException("Only '==', '!=', '<>', 'DISTINCT FROM' or 'IN' are supported in " + ((ReferenceWithFunction) value).getOperation());
            }
            return createFilterClause(EQUALS, value.toFormatlessString(), "");
        }

        @Override
        protected Clause visitIsNotEmptyPredicate(IsNotEmptyPredicate node, Void context) {
            Expression value = node.getValue();
            if (value instanceof ReferenceWithFunction) {
                throw new ParsingException("Only '==', '!=', '<>' or 'IN' are supported in " + ((ReferenceWithFunction) value).getOperation());
            }
            return createFilterClause(NOT_EQUALS, value.toFormatlessString(), "");
        }

        private Clause createFilterClause(Operation operation, String field, String... values) {
            FilterClause filterClause = new ObjectFilterClause();
            filterClause.setOperation(operation);
            filterClause.setField(field);
            filterClause.setValues(Stream.of(values).map(v -> new ObjectFilterClause.Value(ObjectFilterClause.Value.Kind.VALUE, v)).collect(Collectors.toList()));

            return filterClause;
        }

        private Clause createFilterClause(Operation operation, String field, Expression... expressions) {
            FilterClause filterClause = new ObjectFilterClause();
            filterClause.setOperation(operation);
            filterClause.setField(field);
            filterClause.setValues(Stream.of(expressions).map(e -> new ObjectFilterClause.Value(e instanceof Identifier ? ObjectFilterClause.Value.Kind.FIELD : ObjectFilterClause.Value.Kind.VALUE, e.toFormatlessString())).collect(Collectors.toList()));

            return filterClause;
        }
    }
}
