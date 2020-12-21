# Bullet-BQL

[![Build Status](https://travis-ci.org/bullet-db/bullet-bql.svg?branch=master)](https://travis-ci.org/bullet-db/bullet-bql) [![Coverage Status](https://coveralls.io/repos/github/bullet-db/bullet-bql/badge.svg?branch=master)](https://coveralls.io/github/bullet-db/bullet-bql?branch=master) [![Download](https://api.bintray.com/packages/yahoo/maven/bullet-bql/images/download.svg) ](https://bintray.com/yahoo/maven/bullet-bql/_latestVersion)

>BQL is a SQL-like query language specifically designed for [Bullet](https://bullet-db.github.io/) query engine, which provides an easy-to-use yet powerful interactive SQL-like interface. 

This project contains a BQL parser built in [ANTLR 4](http://www.antlr.org/). A BQL query will be parsed, classified, validated and extracted to a [Bullet](https://bullet-db.github.io/) Query or [Bullet JSON](https://bullet-db.github.io/ws/api/).

## Table of Contents

- [Background](#background)
- [Install](#install)
- [Usage](#usage)
- [Date Types](#data-types)
- [Reserved Keywords](#reserved-keywords)
- [Useful Links](#useful-links)
- [Contribute](#contribute)
- [License](#license)
    
## Background

Bullet-BQL is created to provide users with a friendly SQL-like layer to manipulate the power of [Bullet](https://bullet-db.github.io/) query engine. Users can write a simple BQL query instead of [Bullet JSON](https://bullet-db.github.io/ws/api/), which saves time and eases the learning curve for people familiar with SQL.

## Install

* The Bullet-BQL artifact can be obtained from [JCenter](https://bintray.com/bintray/jcenter?filterByPkgName=bullet-bql).

* To run the tool from the command line, first compile and then run:

    `mvn clean compile` 
    
    `mvn exec:java`

    Then you can use bullet-bql to parse BQL strings into [Bullet JSON](https://bullet-db.github.io/ws/api/) queries.
     
* Bullet-BQL is currently being integrated into [Bullet-Service](https://github.com/bullet-db/bullet-service/), and will provide a BQL endpoint directly. 

## Usage

* Build a [Bullet](https://bullet-db.github.io/) Query from a BQL string.

    Simply construct a `BulletQueryBuilder` and call `buildQuery(String bql)`. A [Bullet](https://bullet-db.github.io/) Query is returned.

* You can change the max query length in `BQLConfig` by altering the `.yaml`.

## Data Types

* **Null**: `NULL`.

* **Boolean**: `TRUE`, `FALSE`.

* **Integer**: 32-bit signed two’s complement integer with a minimum value of `-2^31` and a maximum value of `2^31 - 1`. Example: `65`.

* **Long**: 64-bit signed two’s complement integer with a minimum value of `-2^63 + 1` and a maximum value of `2^63 - 1`. Example: `9223372036854775807`, `-9223372036854775807`.

* **Float**: 32-bit inexact, variable-precision with a minimum value of `2^-149` and a maximum value of `(2-2^-23)·2^127`. Example: `1.70141183E+38`, `1.17549435E-38`, `0.15625`.

* **Double**: 64-bit inexact, variable-precision with a minimum value of `2^-1074` and a maximum value of `(2-2^-52)·2^1023`. Example: `1.7976931348623157E+308`, `.17976931348623157E+309`, `4.9E-324`.

* **String**: character string which can have escapes. Example: `'this is a string'`, `'this is ''another'' string'`.

* **ColumnReference**: representation of a column field. Unquoted ColumnReference must start with a letter or `_`. Example: `column_name`, `column_name.foo`, `column_name.foo.bar`, `column_name[0].bar`, or `"123column"`.

* **All**: representation of all columns. Example: `*`.

## Statement Syntax

    SELECT DISTINCT? select_clause
    FROM from_clause
    ( WHERE where_clause )?
    ( GROUP BY groupBy_clause )?
    ( HAVING having_clause )?
    ( ORDER BY orderBy_clause )?
    ( WINDOWING windowing_clause )?
    ( LIMIT limit_clause )?;
    
where `select_clause` is one of
    
    * ( , arithmetic_expr (AS? ColumnReference )? )*
    COUNT( DISTINCT reference_expr ( , reference_expr )* )
    group_function ( AS? ColumnReference )? ( , group_function ( AS? ColumnReference )? )* ( , reference_expr ( AS? ColumnReference )? )* ( , arithmetic_expr (AS? ColumnReference )? )*
    reference_expr ( AS? ColumnReference )? ( , reference_expr ( AS? ColumnReference )? )* ( , arithmetic_expr (AS? ColumnReference )? )*
    arithmetic_expr ( AS? ColumnReference )? ( , arithmetic_expr ( AS? ColumnReference )? )*
    distribution_type( reference_expr, input_mode ) ( AS? ColumnReference )? ( , arithmetic_expr (AS? ColumnReference )? )*
    TOP ( ( Integer | Long ) ( , Integer | Long ) )? , reference_expr ( , reference_expr )? ) ( AS? ColumnReference )? ( , arithmetic_expr (AS? ColumnReference )? )*
    
`reference_expr` is one of `ColumnReference` or `Dereference`.

`arithmetic_expr` is one of
    
    ( arithmetic_expr )
    arithmetic_expr ( * | / | + | - ) arithmetic_expr
    CAST ( arithmetic_expr , ( 'INTEGER' | 'LONG' | 'FLOAT' | 'DOUBLE' | 'BOOLEAN' | 'STRING' ) )
    reference_expr
    Integer | Long | Double | Decimal | Boolean | String
    
and `group_function` is one of `SUM(reference_expr)`, `MIN(reference_expr)`, `MAX(reference_expr)`, `AVG(reference_expr)` and `COUNT(*)`. `reference_expr` is one of ColumnReference and Dereference. `distribution_type` is one of `QUANTILE`, `FREQ` and `CUMFREQ`. The 1st number in `TOP` is K, and the 2nd number is an optional threshold.  The `input_mode` is one of 

    LINEAR, ( Integer | Long )                                              evenly spaced
    REGION, ( Integer | Long ), ( Integer | Long ), ( Integer | Long )      evenly spaced in a region
    MANUAL, ( Integer | Long ) (, ( Integer | Long ) )*                     defined points
    
and `from_clause` is one of

    STREAM()                                                          default time duration will be set from BQLConfig
    STREAM( ( Long | MAX ), TIME )                                    time based duration control. 
    STREAM( ( Long | MAX ), TIME, ( Long | MAX ), RECORD )            time and record based duration control. 

`RECORD` will be supported in the future.

and `where_clause` is one of

    NOT where_clause
    where_clause AND where_clause
    where_clause OR where_clause
    reference_expr IS NOT? NULL
    reference_expr IS NOT? EMPTY
    reference_expr IS NOT? DISTINCT FROM value_expr
    reference_expr NOT? BETWEEN value_expr AND value_expr
    reference_expr NOT? IN ( value_expr ( , value_expr )* )
    reference_expr NOT? LIKE ( value_expr ( , value_expr )* )
    reference_expr NOT? CONTAINSKEY ( value_expr ( , value_expr )* )
    reference_expr NOT? CONTAINSVALUE ( value_expr ( , value_expr )* )
    reference_expr ( = | <> | != | < | > | <= | >= ) value_expr
    SIZEOF(reference_expr) ( = | <> | != ) value_expr
    SIZEOF(reference_expr) NOT? IN ( value_expr ( , value_expr )* )
    SIZEOF(reference_expr) NOT? DISTINCT FROM value_expr
  
`value_expr` is one of Null, Boolean, Integer, Long, Double, Decimal, String or `reference_expr`.

and `groupBy_clause` is one of

    ()                                                                group all
    reference_expr ( , reference_expr )*                              group by
    ( reference_expr ( , reference_expr )* )                          group by    

and `HAVING` and `ORDER BY` together is only supported for TopK. In which case, `having_clause` is 

    COUNT(*) >= Integer
    
and `orderBy_clause` is

    COUNT(*) DESC
    
If not TopK, `HAVING` is not supported, and `orderBy_clause` is

    reference_expr (ASC | DESC)? ( , reference_expr (ASC | DESC)? )*

and `windowing_clause` is one of 

    ( EVERY, ( Integer | Long ), ( TIME | RECORD ), include )
    ( TUMBLING, ( Integer | Long ), ( TIME | RECORD ) )

`include` is one of 

    ALL
    FIRST, ( Integer | Long ), ( TIME | RECORD )
    LAST, ( Integer | Long ), ( TIME | RECORD )                       will be supported

and `limit_clause` is one of

    Integer | Long 
    ALL                                                               will be supported

### Simplest Query

    SELECT *
    FROM STREAM(30000, TIME)
    LIMIT 1;

### Simple Filtering

    SELECT *
    FROM STREAM(30000, TIME)
    WHERE id = 'btsg8l9b234ha'
    LIMIT 1;

### SIZEOF Filtering

    SELECT *
    FROM STREAM(30000, TIME)
    WHERE SIZEOF(id_map) = 4
    LIMIT 1;

### CONTAINSKEY Filtering

    SELECT *
    FROM STREAM(30000, TIME)
    WHERE id_map CONTAINSKEY ("key")
    LIMIT 1;

### CONTAINSVALUE Filtering

    SELECT *
    FROM STREAM(30000, TIME)
    WHERE id_map NOT CONTAINSVALUE ("btsg8l9b234ha")
    LIMIT 1;

### Compare to other fields Filtering

    SELECT *
    FROM STREAM(30000, TIME)
    WHERE id = uid
    LIMIT 1;
     
### Relational & Logical Filters and Projections

    SELECT timestamp AS ts, device_timestamp AS device_ts, 
           event AS event, page_domain AS domain, page_id AS id
    FROM STREAM(20000, TIME)
    WHERE id = 'btsg8l9b234ha' AND page_id IS NOT NULL
    LIMIT 10;

### GROUP ALL COUNT Aggregation

    SELECT COUNT(*) AS numSeniors
    FROM STREAM(20000, TIME)
    WHERE demographics.age > 65;

### GROUP ALL Multiple Aggregations

    SELECT COUNT(*) AS numCalifornians, AVG(demographics.age) AS avgAge, 
           MIN(demographics.age) AS minAge, MAX(demographics.age) AS maxAge
    FROM STREAM(20000, TIME)
    WHERE demographics.state = 'california';

### COUNT DISTINCT Aggregation

    SELECT COUNT(DISTINCT browser_name, browser_version) AS "COUNT DISTINCT"
    FROM STREAM(10000, TIME);

### DISTINCT Aggregation

    SELECT browser_name AS browser
    FROM STREAM(30000, TIME)
    GROUP BY browser_name
    LIMIT 10;

### GROUP BY Aggregation

    SELECT demographics.country AS country, device AS device,
           COUNT(*) AS count, AVG(demographics.age) AS averageAge,
           AVG(timespent) AS averageTimespent
    FROM STREAM(20000, TIME)
    WHERE demographics IS NOT NULL
    GROUP BY demographics.country, device
    LIMIT 50;

### QUANTILE Distribution Aggregation

    SELECT QUANTILE(duration, LINEAR, 11)
    FROM STREAM(5000, TIME)
    LIMIT 11;

### FREQ(Frequencies) Distribution Aggregation

    SELECT FREQ(duration, REGION, 2000, 20000, 500)
    FROM STREAM(5000, TIME)
    LIMIT 100;

### CUMFREQ(Cumulative Frequencies) Distribution Aggregation

    SELECT CUMFREQ(duration, MANUAL, 20000, 2000, 15000, 45000)
    FROM STREAM(5000, TIME)
    LIMIT 100;

### TOP K Aggregation

    SELECT TOP(500, 100, demographics.country, browser_name) AS numEvents
    FROM STREAM(10000, TIME)
    WHERE demographics.country IS NOT NULL AND browser_name IS NOT NULL;
    
Or

    SELECT demographics.country, browser_name, COUNT(*) AS numEvents
    FROM STREAM(10000, TIME)
    WHERE demographics.country IS NOT NULL AND browser_name IS NOT NULL
    GROUP BY demographics.country, browser_name
    HAVING COUNT(*) >= 100
    ORDER BY COUNT(*) DESC
    LIMIT 500;

### Computation

    SELECT TOP(500, 100, demographics.country, browser_name) AS numEvents, numEvents * 100 AS inflatedNumEvents
    FROM STREAM(10000, TIME);
    
### Order By

    SELECT DISTINCT browser_name
    FROM STREAM(30000, TIME)
    ORDER BY browser_name;

## Useful links

* [Spark Quick Start](https://bullet-db.github.io/quick-start/spark) to start with a Bullet instance running locally on Spark.
* [Storm Quick Start](https://bullet-db.github.io/quick-start/storm) to start with a Bullet instance running locally on Storm.
* [Spark Architecture](https://bullet-db.github.io/backend/spark-architecture/) to see how Bullet is implemented on Storm.
* [Storm Architecture](https://bullet-db.github.io/backend/storm-architecture/) to see how Bullet is implemented on Storm.
* [Setup on Spark](https://bullet-db.github.io/backend/spark-setup/) to see how to setup Bullet on Spark.
* [Setup on Storm](https://bullet-db.github.io/backend/storm-setup/) to see how to setup Bullet on Storm.
* [API Examples](https://bullet-db.github.io/ws/examples/) to see what kind of queries you can run on Bullet.
* [Setup Web Service](https://bullet-db.github.io/ws/setup/) to setup the Bullet Web Service.
* [Setup UI](https://bullet-db.github.io/ui/setup/) to setup the Bullet UI.

## Contribute

Please refer to [the contributing.md file](Contributing.md) for information about how to get involved. We welcome issues, questions, and pull requests. Pull Requests are welcome.

## License

This project is licensed under the terms of the [Apache 2.0](LICENSE-Apache-2.0) open source license. See LICENSE file for terms.
