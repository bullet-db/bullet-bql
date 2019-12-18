/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.tree.WindowIncludeNode;
import com.yahoo.bullet.bql.tree.WindowNode;
import com.yahoo.bullet.parsing.Window;
import com.yahoo.bullet.parsing.Window.Unit;

import java.util.HashMap;
import java.util.Map;

import static com.yahoo.bullet.parsing.Window.EMIT_EVERY_FIELD;
import static com.yahoo.bullet.parsing.Window.INCLUDE_FIRST_FIELD;
import static com.yahoo.bullet.parsing.Window.TYPE_FIELD;
import static com.yahoo.bullet.parsing.Window.Unit.ALL;
import static java.util.Objects.requireNonNull;

public class WindowExtractor {
    private WindowNode node;

    /**
     * Constructor that requires a {@link WindowNode}.
     *
     * @param node A non-null {@link WindowNode}.
     * @throws NullPointerException when node is null.
     */
    public WindowExtractor(WindowNode node) throws NullPointerException {
        requireNonNull(node);

        this.node = node;
    }

    /**
     * Extract {@link Window} from a {@link WindowNode} node.
     *
     * @return A {@link Window}.
     */
    public Window extractWindow() {
        Window window = new Window();
        window.setEmit(extractEmit(node));
        window.setInclude(extractInclude(node.getInclude()));
        return window;
    }

    private Map<String, Object> extractEmit(WindowNode node) {
        Map<String, Object> emit = new HashMap<>();
        emit.put(EMIT_EVERY_FIELD, node.getEmitEvery());
        emit.put(TYPE_FIELD, node.getEmitType());
        return emit;
    }

    private Map<String, Object> extractInclude(WindowIncludeNode node) {
        Map<String, Object> include = new HashMap<>();
        Unit unit = node.getUnit();
        include.put(TYPE_FIELD, unit);
        if (unit != ALL) {
            include.put(INCLUDE_FIRST_FIELD, node.getNumber().get());
        }
        return include;
    }
}
