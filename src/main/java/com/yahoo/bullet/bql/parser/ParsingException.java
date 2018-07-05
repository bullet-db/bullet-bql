/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/parser/ParsingException.java
 */
package com.yahoo.bullet.bql.parser;

import com.yahoo.bullet.bql.tree.NodeLocation;
import org.antlr.v4.runtime.RecognitionException;

import static java.lang.String.format;

public class ParsingException extends RuntimeException {
    private final int line;
    private final int charPositionInLine;

    /**
     * Constructor to initialize ParsingException with a message, reason, line number and char position in line.
     *
     * @param message            The error message to be associated with the ParsingException.
     * @param cause              The reason for the ParsingException.
     * @param line               The line number of the reason.
     * @param charPositionInLine The char position in line of the reason.
     */
    public ParsingException(String message, RecognitionException cause, int line, int charPositionInLine) {
        super(message, cause);

        this.line = line;
        this.charPositionInLine = charPositionInLine;
    }

    /**
     * Constructor to initialize ParsingException with a message.
     *
     * @param message The error message to be associated with the ParsingException.
     */
    public ParsingException(String message) {
        this(message, null, 1, 0);
    }

    /**
     * Constructor to initialize ParsingException with a message and {@link NodeLocation}.
     *
     * @param message      The error message to be associated with the ParsingException.
     * @param nodeLocation The {@link NodeLocation} that contains line number and char position in line.
     */
    public ParsingException(String message, NodeLocation nodeLocation) {
        this(message, null, nodeLocation.getLineNumber(), nodeLocation.getColumnNumber());
    }

    @Override
    public String getMessage() {
        return format("line %s:%s: %s", line, charPositionInLine + 1, super.getMessage());
    }
}
