package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.tree.ASTVisitor;
import com.yahoo.bullet.bql.tree.BinaryExpression;
import com.yahoo.bullet.bql.tree.CastExpression;
import com.yahoo.bullet.bql.tree.Expression;
import com.yahoo.bullet.bql.tree.Identifier;
import com.yahoo.bullet.bql.tree.LeafExpression;
import com.yahoo.bullet.bql.tree.Node;
import com.yahoo.bullet.bql.tree.SelectItem;
import com.yahoo.bullet.parsing.Computation;
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
     * @throws NullPointerException when any of selectFields and aliases is null.
     */
    public ComputationExtractor(Set<Expression> computations, Map<Node, Identifier> aliases) throws NullPointerException {
        requireNonNull(computations);
        requireNonNull(aliases);

        this.computations = computations;
        this.aliases = aliases;
    }

    /**
     * Extract a List of Computation PostAggregations.
     *
     * @return A Projection based on selectFields and aliases.
     */
    public List<PostAggregation> extractComputations() {
        ExtractVisitor visitor = new ExtractVisitor(aliases);

        return computations.stream().map(node -> {
                Computation computation = new Computation();
                computation.setType(PostAggregation.Type.COMPUTATION);
                computation.setExpression(visitor.process(node));
                computation.setNewName(getAlias(node));
                return computation;
            }).collect(Collectors.toList());
    }

    private String getAlias(Expression column) {
        if (aliases.containsKey(column)) {
            return aliases.get(column).toFormatlessString();
        } else {
            return column.toFormatlessString();
        }
    }

    private static class ExtractVisitor extends ASTVisitor<com.yahoo.bullet.parsing.Expression, Void> {
        private Map<Node, Identifier> aliases;

        ExtractVisitor(Map<Node, Identifier> aliases) {
            this.aliases = aliases;
        }

        protected com.yahoo.bullet.parsing.Expression visitCastExpression(CastExpression node, Void context) throws ParsingException {
            if (node.getExpression() instanceof BinaryExpression) {
                com.yahoo.bullet.parsing.BinaryExpression expression = (com.yahoo.bullet.parsing.BinaryExpression) process(node.getExpression());
                expression.setType(Type.valueOf(node.getCastType().toUpperCase()));
                return expression;
            } else if (node.getExpression() instanceof LeafExpression) {
                com.yahoo.bullet.parsing.LeafExpression expression = (com.yahoo.bullet.parsing.LeafExpression) process(node.getExpression());
                Value value = expression.getValue();
                expression.setValue(new Value(value.getKind(), value.getValue(), Type.valueOf(node.getCastType().toUpperCase())));
                return expression;
            }
            throw new ParsingException("Only casting of binary and leaf expressions supported");
        }

        protected com.yahoo.bullet.parsing.Expression visitBinaryExpression(BinaryExpression node, Void context) throws ParsingException {
            com.yahoo.bullet.parsing.BinaryExpression binaryExpression = new com.yahoo.bullet.parsing.BinaryExpression();
            binaryExpression.setLeft(process(node.getLeft()));
            binaryExpression.setRight(process(node.getRight()));
            switch (node.getOp()) {
                case "+":
                    binaryExpression.setOperation(com.yahoo.bullet.parsing.Expression.Operation.ADD);
                    break;
                case "-":
                    binaryExpression.setOperation(com.yahoo.bullet.parsing.Expression.Operation.SUB);
                    break;
                case "*":
                    binaryExpression.setOperation(com.yahoo.bullet.parsing.Expression.Operation.MUL);
                    break;
                case "/":
                    binaryExpression.setOperation(com.yahoo.bullet.parsing.Expression.Operation.DIV);
                    break;
                default:
                    throw new ParsingException("Only +, -, *, / supported");
            }
            return binaryExpression;
        }

        protected com.yahoo.bullet.parsing.Expression visitLeafExpression(LeafExpression node, Void context) {
            com.yahoo.bullet.parsing.LeafExpression leafExpression = new com.yahoo.bullet.parsing.LeafExpression();
            Expression value = node.getValue();
            switch (value.getType(SelectItem.Type.class)) {
                case COLUMN:
                case SUB_COLUMN:
                    leafExpression.setValue(new Value(Value.Kind.FIELD, value.toFormatlessString()));
                    break;
                default:
                    leafExpression.setValue(new Value(Value.Kind.VALUE, value.toFormatlessString()));
            }
            return leafExpression;
        }
    }
}