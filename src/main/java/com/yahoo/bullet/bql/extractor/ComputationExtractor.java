/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.tree.ASTVisitor;
import com.yahoo.bullet.bql.tree.ArithmeticUnaryExpression;
import com.yahoo.bullet.bql.tree.InfixExpression;
import com.yahoo.bullet.bql.tree.CastExpression;
import com.yahoo.bullet.bql.tree.DereferenceExpression;
import com.yahoo.bullet.bql.tree.Expression;
import com.yahoo.bullet.bql.tree.Identifier;
import com.yahoo.bullet.bql.tree.Literal;
import com.yahoo.bullet.bql.tree.Node;
import com.yahoo.bullet.bql.tree.ParensExpression;
import com.yahoo.bullet.parsing.BinaryExpression;
import com.yahoo.bullet.parsing.Computation;
import com.yahoo.bullet.parsing.Expression.Operation;
import com.yahoo.bullet.parsing.LeafExpression;
import com.yahoo.bullet.parsing.PostAggregation;
import com.yahoo.bullet.parsing.Value;
import com.yahoo.bullet.typesystem.Type;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class ComputationExtractor {
    private Set<Expression> computations;
    private Map<Node, Identifier> aliases;

    /**
     * Constructor that requires a set of selectFields and map of aliases.
     *
     * @param computations The non-null Set of computations.
     * @param aliases      The non-null Map of aliases.
     */
    public ComputationExtractor(Set<Expression> computations, Map<Node, Identifier> aliases) {
        requireNonNull(computations);
        requireNonNull(aliases);
        this.computations = computations;
        this.aliases = aliases;
    }

    /**
     * Extract a List of Computation PostAggregations.
     *
     * @return A List of PostAggregations based on computations and aliases.
     */
    public List<PostAggregation> extractComputations() {
        ExtractVisitor visitor = new ExtractVisitor();

        return computations.stream().map(node -> {
                Computation computation = new Computation();
                computation.setType(PostAggregation.Type.COMPUTATION);
                computation.setExpression(visitor.process(node));
                computation.setNewName(getNewName(node));
                return computation;
            }).collect(Collectors.toList());
    }

    private String getNewName(Expression column) {
        return aliases.containsKey(column) ? aliases.get(column).toFormatlessString() : column.toFormatlessString();
    }

    private static class ExtractVisitor extends ASTVisitor<com.yahoo.bullet.parsing.Expression, Void> {
        private static final String ADD = "+";
        private static final String SUB = "-";
        private static final String MUL = "*";
        private static final String DIV = "/";

        protected com.yahoo.bullet.parsing.Expression visitCastExpression(CastExpression node, Void context) throws ParsingException {
            if (!(node.getExpression() instanceof CastExpression)) {
                com.yahoo.bullet.parsing.Expression expression = process(node.getExpression());
                if (expression instanceof BinaryExpression) {
                    ((BinaryExpression) expression).setType(Type.valueOf(node.getCastType().toUpperCase()));
                    return expression;
                }
                if (expression instanceof LeafExpression) {
                    Value value = ((LeafExpression) expression).getValue();
                    ((LeafExpression) expression).setValue(new Value(value.getKind(), value.getValue(), Type.valueOf(node.getCastType().toUpperCase())));
                    return expression;
                }
            }
            throw new ParsingException("Only casting of binary and leaf expressions supported");
        }

        protected com.yahoo.bullet.parsing.Expression visitBinaryExpression(InfixExpression node, Void context) throws ParsingException {
            BinaryExpression binaryExpression = new BinaryExpression();
            binaryExpression.setLeft(process(node.getLeft()));
            binaryExpression.setRight(process(node.getRight()));
            switch (node.getOp()) {
                case ADD:
                    binaryExpression.setOperation(Operation.ADD);
                    break;
                case SUB:
                    binaryExpression.setOperation(Operation.SUB);
                    break;
                case MUL:
                    binaryExpression.setOperation(Operation.MUL);
                    break;
                case DIV:
                    binaryExpression.setOperation(Operation.DIV);
                    break;
                default:
                    throw new ParsingException("Only +, -, *, / supported");
            }
            return binaryExpression;
        }

        @Override
        protected com.yahoo.bullet.parsing.Expression visitParensExpression(ParensExpression node, Void context) {
            if (node.getValue() instanceof CastExpression) {
                throw new ParsingException("CAST cannot be surrounded in parentheses.");
            }
            return process(node.getValue());
        }

        @Override
        protected com.yahoo.bullet.parsing.Expression visitLiteral(Literal node, Void context) {
            LeafExpression leafExpression = new LeafExpression();
            leafExpression.setValue(new Value(Value.Kind.VALUE, node.toFormatlessString()));
            return leafExpression;
        }

        @Override
        protected com.yahoo.bullet.parsing.Expression visitArithmeticUnary(ArithmeticUnaryExpression node, Void context) {
            LeafExpression leafExpression = new LeafExpression();
            leafExpression.setValue(new Value(Value.Kind.VALUE, node.toFormatlessString()));
            return leafExpression;
        }

        @Override
        protected com.yahoo.bullet.parsing.Expression visitIdentifier(Identifier node, Void context) {
            LeafExpression leafExpression = new LeafExpression();
            leafExpression.setValue(new Value(Value.Kind.FIELD, node.toFormatlessString()));
            return leafExpression;
        }

        @Override
        protected com.yahoo.bullet.parsing.Expression visitDereferenceExpression(DereferenceExpression node, Void context) {
            LeafExpression leafExpression = new LeafExpression();
            leafExpression.setValue(new Value(Value.Kind.FIELD, node.toFormatlessString()));
            return leafExpression;
        }
    }
}
