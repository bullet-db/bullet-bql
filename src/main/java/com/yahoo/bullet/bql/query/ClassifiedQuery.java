package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.bql.BQLConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.parsing.Query;
import lombok.RequiredArgsConstructor;

import java.util.List;

@RequiredArgsConstructor
public abstract class ClassifiedQuery {
    protected final ProcessedQuery processedQuery;

    public abstract List<BulletError> validate();

    public abstract Query extractQuery(BQLConfig config);
}
