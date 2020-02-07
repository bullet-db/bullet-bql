package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.bql.BQLConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.parsing.Query;

import java.util.List;

public class InvalidQuery extends ClassifiedQuery {
    public InvalidQuery(ProcessedQuery processedQuery) {
        super(processedQuery);
    }

    @Override
    public List<BulletError> validate() {
        return processedQuery.getErrors();
    }

    @Override
    public Query extractQuery(BQLConfig config) {
        return null;
    }
}
