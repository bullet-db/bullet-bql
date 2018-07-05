/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/parser/StatementSplitter.java
 */
package com.yahoo.bullet.bql.parser;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenSource;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class StatementSplitter {
    private final List<Statement> completeStatements;
    private final String partialStatement;

    /**
     * Constructor that requires a String which can contain multiple BQL queries.
     *
     * @param bql A String consists of multiple BQL queries, separated by ";".
     */
    public StatementSplitter(String bql) {
        this(bql, ImmutableSet.of(";"));
    }

    /**
     * Constructor that requires a String and String Set of delimiters.
     *
     * @param bql        A String consists of multiple BQL queries, separated by delimiters
     * @param delimiters String Set of delimiters that separate BQL queries
     */
    public StatementSplitter(String bql, Set<String> delimiters) {
        TokenSource tokens = getLexer(bql, delimiters);
        ImmutableList.Builder<Statement> list = ImmutableList.builder();
        StringBuilder sb = new StringBuilder();
        while (true) {
            Token token = tokens.nextToken();
            if (token.getType() == Token.EOF) {
                break;
            }
            if (token.getType() == BQLBaseParser.DELIMITER) {
                String statement = sb.toString().trim();
                if (!statement.isEmpty()) {
                    list.add(new Statement(statement, token.getText()));
                }
                sb = new StringBuilder();
            } else {
                sb.append(token.getText());
            }
        }
        this.completeStatements = list.build();
        this.partialStatement = sb.toString().trim();
    }

    /**
     * Get List of complete {@link Statement}.
     *
     * @return {@link #completeStatements}.
     */
    public List<Statement> getCompleteStatements() {
        return completeStatements;
    }

    /**
     * Get a String of partial {@link Statement}.
     *
     * @return {@link #partialStatement}.
     */
    public String getPartialStatement() {
        return partialStatement;
    }

    /**
     * Squeeze a statement String. Remove \n and replace multiple space with one white space.
     *
     * @param bql The statement String.
     * @return A squeezed statement String without \n or multiple white space.
     */
    public static String squeezeStatement(String bql) {
        TokenSource tokens = getLexer(bql, ImmutableSet.of());
        StringBuilder sb = new StringBuilder();
        while (true) {
            Token token = tokens.nextToken();
            if (token.getType() == Token.EOF) {
                break;
            }
            if (token.getType() == BQLBaseLexer.WS) {
                sb.append(' ');
            } else {
                sb.append(token.getText());
            }
        }
        return sb.toString().trim();
    }

    /**
     * Check if a statement String is empty, which may contain only white space or comment.
     *
     * @param bql The statement String.
     * @return true if statement String contains BQL, otherwise return false.
     */
    public static boolean isEmptyStatement(String bql) {
        TokenSource tokens = getLexer(bql, ImmutableSet.of());
        while (true) {
            Token token = tokens.nextToken();
            if (token.getType() == Token.EOF) {
                return true;
            }
            if (token.getChannel() != Token.HIDDEN_CHANNEL) {
                return false;
            }
        }
    }

    private static TokenSource getLexer(String bql, Set<String> terminators) {
        requireNonNull(bql, "bql String is null");
        CharStream stream = new CaseInsensitiveStream(new ANTLRInputStream(bql));
        return new DelimiterLexer(stream, terminators);
    }

    public static class Statement {
        private final String statement;
        private final String terminator;

        /**
         * Constructor that require a BQL statement String and terminator String.
         *
         * @param statement  The non-null BQL statement String.
         * @param terminator The non-null terminator String used in the statement String.
         */
        public Statement(String statement, String terminator) {
            this.statement = requireNonNull(statement, "Statement String is null");
            this.terminator = requireNonNull(terminator, "Terminator String is null");
        }

        /**
         * Get the {@link #statement} String of this Statement.
         *
         * @return {@link #statement}.
         */
        public String statement() {
            return statement;
        }

        /**
         * Get the {@link #terminator} String of this Statement.
         *
         * @return {@link #terminator}.
         */
        public String terminator() {
            return terminator;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }
            Statement o = (Statement) obj;
            return Objects.equals(statement, o.statement) &&
                    Objects.equals(terminator, o.terminator);
        }

        @Override
        public int hashCode() {
            return Objects.hash(statement, terminator);
        }

        @Override
        public String toString() {
            return statement + terminator;
        }
    }
}
