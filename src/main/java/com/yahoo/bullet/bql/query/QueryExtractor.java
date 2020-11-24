/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.bql.tree.QueryNode;
import com.yahoo.bullet.bql.tree.StreamNode;
import com.yahoo.bullet.bql.tree.WindowIncludeNode;
import com.yahoo.bullet.bql.tree.WindowNode;
import com.yahoo.bullet.query.Window;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class QueryExtractor {
    private static final String MAX = "MAX";

    private Window window;
    private Long duration;
    private Integer limit;

    public QueryExtractor(QueryNode queryNode) {
        window = extractWindow(queryNode.getWindow());
        duration = extractDuration(queryNode.getStream());
        limit = extractLimit(queryNode.getLimit());
    }

    private static Window extractWindow(WindowNode windowNode) {
        if (windowNode == null) {
            return new Window();
        }
        WindowIncludeNode windowInclude = windowNode.getWindowInclude();
        if (windowInclude == null) {
            return new Window(windowNode.getEmitEvery(), windowNode.getEmitType());
        }
        return new Window(windowNode.getEmitEvery(), windowNode.getEmitType(), windowInclude.getIncludeUnit(), windowInclude.getFirst());
    }

    private static Long extractDuration(StreamNode streamNode) {
        String timeDuration = streamNode.getTimeDuration();
        if (timeDuration != null) {
            return timeDuration.equalsIgnoreCase(MAX) ? Long.MAX_VALUE : Long.parseLong(timeDuration);
        }
        return null;
    }

    public static Integer extractLimit(String limit) {
        return limit != null ? Integer.parseInt(limit) : null;
    }
}
