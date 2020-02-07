package com.yahoo.bullet.bql.query;

public class QueryClassifier {
    public static ClassifiedQuery classify(ProcessedQuery processedQuery) {
        switch (processedQuery.getQueryType()) {
            case SELECT:
                return new SelectQuery(processedQuery);
            case SELECT_ALL:
                return new SelectAllQuery(processedQuery);
            case SELECT_DISTINCT:
                return new SelectDistinctQuery(processedQuery);
            case COUNT_DISTINCT:
                return new CountDistinctQuery(processedQuery);
            case GROUP:
                return new GroupQuery(processedQuery);
            case DISTRIBUTION:
                return new DistributionQuery(processedQuery);
            case TOP_K:
                return new TopKQuery(processedQuery);
            case SPECIAL_K:
                return new SpecialKQuery(processedQuery);
            default:
                return new InvalidQuery(processedQuery);
        }
    }
}
