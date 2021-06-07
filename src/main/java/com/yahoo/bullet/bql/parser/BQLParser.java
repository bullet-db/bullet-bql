/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/parser/SqlParser.java
 */
package com.yahoo.bullet.bql.parser;

import com.yahoo.bullet.bql.tree.Node;
import com.yahoo.bullet.bql.tree.QueryNode;
import lombok.AllArgsConstructor;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.Pair;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.List;
import java.util.function.Function;

import static java.util.Arrays.asList;

public class BQLParser {
    private static final BaseErrorListener ERROR_LISTENER = new BaseErrorListener() {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String message, RecognitionException e) {
            throw new ParsingException(message, e, line, charPositionInLine);
        }
    };

    /**
     * Create a {@link QueryNode} which is a {@link Node} Tree from given BQL String.
     *
     * @param bql A BQL String.
     * @return A {@link QueryNode} which is a {@link Node} Tree.
     * @throws ParsingException when query is not valid.
     */
    public QueryNode createQueryNode(String bql) {
        return (QueryNode) invokeParser(bql, BQLBaseParser::statement);
    }

    private Node invokeParser(String bql, Function<BQLBaseParser, ParserRuleContext> parseFunction) {
        try {
            BQLBaseLexer lexer = new BQLBaseLexer(new CaseInsensitiveStream(new ANTLRInputStream(bql)));
            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
            BQLBaseParser parser = new BQLBaseParser(tokenStream);

            parser.addParseListener(new PostProcessor(asList(parser.getRuleNames())));

            lexer.removeErrorListeners();
            lexer.addErrorListener(ERROR_LISTENER);

            parser.removeErrorListeners();
            parser.addErrorListener(ERROR_LISTENER);

            ParserRuleContext tree;
            try {
                // First, try parsing with potentially faster SLL mode.
                parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
                tree = parseFunction.apply(parser);
            } catch (ParseCancellationException ex) {
                // If we fail, parse with LL mode.
                tokenStream.reset(); // rewind input stream.
                parser.reset();
                parser.getInterpreter().setPredictionMode(PredictionMode.LL);
                tree = parseFunction.apply(parser);
            }
            return new ASTBuilder().visit(tree);
        } catch (StackOverflowError e) {
            throw new ParsingException("Stack overflow while parsing.");
        }
    }

    @AllArgsConstructor
    private class PostProcessor extends BQLBaseBaseListener {
        private final List<String> ruleNames;

        @Override
        public void exitDigitIdentifier(BQLBaseParser.DigitIdentifierContext context) throws ParsingException {
            Token token = context.DIGIT_IDENTIFIER().getSymbol();
            throw new ParsingException("Identifiers must not start with a digit; surround the identifier with double quotes.", null, token.getLine(), token.getCharPositionInLine());
        }

        @Override
        public void exitQuotedIdentifier(BQLBaseParser.QuotedIdentifierContext context) {
            Token token = context.QUOTED_IDENTIFIER().getSymbol();
            if (token.getText().equals("\"\"")) {
                throw new ParsingException("Identifiers must not be empty strings.", null, token.getLine(), token.getCharPositionInLine());
            }
        }

        @Override
        public void exitNonReserved(BQLBaseParser.NonReservedContext context) {
            // We can't modify the tree during rule enter/exit event handling unless we're dealing with a terminal.
            // Otherwise, ANTLR gets confused and fires spurious notifications.
            if (!(context.getChild(0) instanceof TerminalNode)) {
                int rule = ((ParserRuleContext) context.getChild(0)).getRuleIndex();
                throw new AssertionError("NonReserved can only contain tokens. Found nested rule: " + ruleNames.get(rule));
            }

            // Replace nonReserved words with IDENT tokens.
            context.getParent().removeLastChild();

            Token token = (Token) context.getChild(0).getPayload();
            context.getParent().addChild(new CommonToken(new Pair<>(token.getTokenSource(), token.getInputStream()), BQLBaseLexer.IDENTIFIER, token.getChannel(), token.getStartIndex(), token.getStopIndex()));
        }
    }
}
