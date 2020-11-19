/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.query.ProcessedQuery;
import com.yahoo.bullet.bql.tree.QueryNode;
import com.yahoo.bullet.query.Window;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryExtractor {
    private static Window extractWindow(ProcessedQuery processedQuery) {
        return WindowExtractor.extractWindow(processedQuery);
    }

    private static Long extractDuration(ProcessedQuery processedQuery) {
        return processedQuery.getTimeDuration();
    }

    public static Integer extractLimit(QueryNode queryNode) {
        return queryNode.getLimit() != null ? Integer.parseInt(queryNode.getLimit()) : null;
    }
}
