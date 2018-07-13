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

* To run the tool from the command line, first build the jar:

    `mvn clean package` 

    Then you can use bullet-bql to parse BQL strings into [Bullet JSON](https://bullet-db.github.io/ws/api/) queries. For example:
     
    `mvn exec:java -Dexec.args="'SELECT * FROM STREAM(30000, TIME) LIMIT 1;'"` 

    **Note:** Notice the query must be enclosed in single+double quotes: `"'` so each word is not interpreted as it's own argument.
     
* Bullet-BQL is currently being integrated into [Bullet-Service](https://github.com/bullet-db/bullet-service/), and will provide a BQL endpoint directly. 

## Usage

* Build a [Bullet](https://bullet-db.github.io/) Query from a BQL string.

    Simply construct a `BulletQueryBuilder` and call `buildQuery(String bql)`. A [Bullet](https://bullet-db.github.io/) Query is returned.
    
* Build a [Bullet JSON](https://bullet-db.github.io/ws/api/) from a BQL string.

    Simply construct a `BulletQueryBuilder` and call `buildJson(String bql)`. A [Bullet JSON](https://bullet-db.github.io/ws/api/) is returned.
    
* You can change `BQLConfig` by altering `.yaml` to change BQL delimiter and how parser treats decimal numbers.

## Data Types

* **Null**: `NULL`.

* **Boolean**: `TRUE`, `FALSE`.

* **Integer**: 32-bit signed two’s complement integer with a minimum value of `-2^31` and a maximum value of `2^31 - 1`. Example: `65`.

* **Long**: 64-bit signed two’s complement integer with a minimum value of `-2^63 + 1` and a maximum value of `2^63 - 1`. Example: `9223372036854775807`, `-9223372036854775807`.

* **Double**: 64-bit inexact, variable-precision with a minimum value of `2^-1074` and a maximum value of `(2-2^-52)·2^1023`. Example: `1.7976931348623157E+308`, `.17976931348623157E+309`, `4.9E-324`.

* **Decimal**: decimal number can be treated as Double, String or ParsingException. This is controlled by `ParsingOptions`. `1.7976931348623157`, `.17976931348623157`.

* **String**: character string which can have escapes. Example: `'this is a string'`, `'this is ''another'' string'`.

* **ColumnReference**: representation of a single column. Unquoted ColumnReference must start with a letter or `_`. Quoted ColumnReference can have escape. Example: `column_name`, `"#column""with""escape"`.

* **Dereference**: representation of a column field. Example: `column_name.field_name`.

* **All**: representation of all columns. Example: `*`. `column_name.*` is interpreted as `column_name`.

## Reserved Keywords

Reserved keywords must be double quoted in order to be used as ColumnReference or Dereference.

|      Keyword          |    SQL:2016     |   SQL-92      |
| --------------------- | :-------------: | :-----------: |
| `ALTER`               |     reserved    |   reserved    |
| `AND`                 |     reserved    |   reserved    |
| `AS`                  |     reserved    |   reserved    |
| `BETWEEN`             |     reserved    |   reserved    |
| `BY`                  |     reserved    |   reserved    |
| `CASE`                |     reserved    |   reserved    |
| `CAST`                |     reserved    |   reserved    |
| `CONSTRAINT`          |     reserved    |   reserved    |
| `CREATE`              |     reserved    |   reserved    |
| `CROSS`               |     reserved    |   reserved    |
| `CUBE`                |     reserved    |               |
| `CURRENT_DATE`        |     reserved    |   reserved    |
| `CURRENT_TIME`        |     reserved    |   reserved    |
| `CURRENT_TIMESTAMP`   |     reserved    |   reserved    |
| `CURRENT_USER`        |     reserved    |               |
| `DEALLOCATE`          |     reserved    |   reserved    |
| `DELETE`              |     reserved    |   reserved    |
| `DESCRIBE`            |     reserved    |   reserved    |
| `DISTINCT`            |     reserved    |   reserved    |
| `DROP`                |     reserved    |   reserved    |
| `ELSE`                |     reserved    |   reserved    |
| `END`                 |     reserved    |   reserved    |
| `ESCAPE`              |     reserved    |   reserved    |
| `EXCEPT`              |     reserved    |   reserved    |
| `EXECUTE`             |     reserved    |   reserved    |
| `EXISTS`              |     reserved    |   reserved    |
| `EXTRACT`             |     reserved    |   reserved    |
| `FALSE`               |     reserved    |   reserved    |
| `FOR`                 |     reserved    |   reserved    |
| `FROM`                |     reserved    |   reserved    |
| `FULL`                |     reserved    |   reserved    |
| `GROUP`               |     reserved    |   reserved    |
| `GROUPING`            |     reserved    |               |
| `HAVING`              |     reserved    |   reserved    |
| `IN`                  |     reserved    |   reserved    |
| `INNER`               |     reserved    |   reserved    |
| `INSERT`              |     reserved    |   reserved    |
| `INTERSECT`           |     reserved    |   reserved    |
| `INTO`                |     reserved    |   reserved    |
| `IS`                  |     reserved    |   reserved    |
| `JOIN`                |     reserved    |   reserved    |
| `LEFT`                |     reserved    |   reserved    |
| `LIKE`                |     reserved    |   reserved    |
| `LOCALTIME`           |     reserved    |               |
| `LOCALTIMESTAMP`      |     reserved    |               |
| `NATURAL`             |     reserved    |   reserved    |
| `NORMALIZE`           |     reserved    |               |
| `NOT`                 |     reserved    |   reserved    |
| `NULL`                |     reserved    |   reserved    |
| `ON`                  |     reserved    |   reserved    |
| `OR`                  |     reserved    |   reserved    |
| `ORDER`               |     reserved    |   reserved    |
| `OUTER`               |     reserved    |   reserved    |
| `PREPARE`             |     reserved    |   reserved    |
| `RECURSIVE`           |     reserved    |               |
| `RIGHT`               |     reserved    |   reserved    |
| `ROLLUP`              |     reserved    |               |
| `SELECT`              |     reserved    |   reserved    |
| `TABLE`               |     reserved    |   reserved    |
| `THEN`                |     reserved    |   reserved    |
| `TRUE`                |     reserved    |   reserved    |
| `UESCAPE`             |     reserved    |               |
| `UNION`               |     reserved    |   reserved    |
| `UNNEST`              |     reserved    |               |
| `USING`               |     reserved    |   reserved    |
| `VALUES`              |     reserved    |   reserved    |
| `WHEN`                |     reserved    |   reserved    |
| `WHERE`               |     reserved    |   reserved    |
| `WITH`                |     reserved    |   reserved    |

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
    
    *
    COUNT( DISTINCT reference_expr ( , reference_expr )? )
    group_function ( AS? ColumnReference )? ( , group_function ( AS? ColumnReference )? )? ( , reference_expr ( AS? ColumnReference )? )?
    reference_expr ( AS? ColumnReference )? ( , reference_expr ( AS? ColumnReference )? )?
    distribution_type( reference_expr, input_mode ) ( AS? ColumnReference )?
    TOP ( ( Integer | Long ) ( , Integer | Long ) )? , reference_expr ( , reference_expr )? ) ( AS? ColumnReference )?
    
    
`group_function` is one of `SUM(reference_expr)`, `MIN(reference_expr)`, `MAX(reference_expr)`, `AVG(reference_expr)` and `COUNT(*)`. `reference_expr` is one of ColumnReference and Dereference. `distribution_type` is one of `QUANTILE`, `FREQ` and `CUMFREQ`. The 1st number in `TOP` is K, and the 2nd number is an optional threshold.  The `input_mode` is one of 

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
    reference_expr ( = | <> | != | < | > | <= | >= ) value_expr
  
`value_expr` is one of Null, Boolean, Integer, Long, Double, Decimal and String. 

and `groupBy_clause` is one of

    ()                                                                group all
    reference_expr ( , reference_expr )*                              group by
    ( reference_expr ( , reference_expr )* )                          group by
    
and `HAVING` and `ORDER BY` are only supported for TopK. In which case, `having_clause` is 

    COUNT(*) >= Integer
    
and `orderBy_clause` is

    COUNT(*)

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

**BQL**

    SELECT *
    FROM STREAM(30000, TIME)
    LIMIT 1;

**Bullet Query**

    {
        "aggregation":{
            "type":"RAW",
            "size":1
        },
        "duration":30000
    }

### Simple Filtering

**BQL**

    SELECT *
    FROM STREAM(30000, TIME)
    WHERE id = 'btsg8l9b234ha'
    LIMIT 1;

**Bullet Query**

    {
        "filters":[
            {
                "field":"id",
                "values":[
                    "btsg8l9b234ha"
                ],
                "operation":"=="
            }
        ],
        "aggregation":{
            "type":"RAW",
            "size":1
        },
        "duration":30000
    }
          
### Relational & Logical Filters and Projections

**BQL**

    SELECT timestamp AS ts, device_timestamp AS device_ts, 
           event AS event, page_domain AS domain, page_id AS id
    FROM STREAM(20000, TIME)
    WHERE id = 'btsg8l9b234ha' AND page_id IS NOT NULL
    LIMIT 10;
    
**Bullet Query**

    {
        "filters":[
            {
                "clauses":[
                    {
                        "field":"id",
                        "values":[
                            "btsg8l9b234ha"
                        ],
                        "operation":"=="
                    },
                    {
                        "field":"page_id",
                        "values":[
                            "NULL"
                        ],
                        "operation":"!="
                    }
                ],
                "operation":"AND"
            }
        ],
        "projection":{
            "fields":{
                "page_domain":"domain",
                "page_id":"id",
                "device_timestamp":"device_ts",
                "event":"event",
                "timestamp":"ts"
            }
        },
        "aggregation":{
            "type":"RAW","size":10
        },
        "duration":20000
    }
    
### GROUP ALL COUNT Aggregation

**BQL**

    SELECT COUNT(*) AS numSeniors
    FROM STREAM(20000, TIME)
    WHERE demographics.age > 65
    GROUP BY ();
    
**Bullet Query**

    {
        "filters":[
            {
                "field":"demographics.age",
                "values":[
                    "65"
                ],
                "operation":">"
            }
        ],
        "aggregation":{
            "type":"GROUP",
            "size":500,
            "attributes":{
                "operations":[
                    {
                        "newName":"numSeniors",
                        "type":"COUNT"
                    }
                ]
            }
        },
        "duration":20000
    }
    
### GROUP ALL Multiple Aggregations

**BQL**

    SELECT COUNT(*) AS numCalifornians, AVG(demographics.age) AS avgAge, 
           MIN(demographics.age) AS minAge, MAX(demographics.age) AS maxAge
    FROM STREAM(20000, TIME)
    WHERE demographics.state = 'california'
    GROUP BY ();
    
**Bullet Query**

    {
        "filters":[
            {
                "field":"demographics.state",
                "values":[
                    "california"
                ],
                "operation":"=="
            }
        ],
        "aggregation":{
            "type":"GROUP",
            "size":500,
            "attributes":{
                "operations":[
                    {
                        "newName":"numCalifornians",
                        "type":"COUNT"
                    },
                    {
                        "newName":"minAge",
                        "field":"demographics.age",
                        "type":"MIN"
                    },
                    {
                        "newName":"avgAge",
                        "field":"demographics.age",
                        "type":"AVG"
                    },
                    {
                        "newName":"maxAge",
                        "field":"demographics.age",
                        "type":"MAX"
                    }
                ]
            }
        },
        "duration":20000
    }
    
### COUNT DISTINCT Aggregation

**BQL**

    SELECT COUNT(DISTINCT browser_name, browser_version) AS "COUNT DISTINCT"
    FROM STREAM(10000, TIME);
  
**Bullet Query**

    {
        "aggregation":{
            "type":"COUNT DISTINCT",
            "size":500,
            "fields":{
                "browser_name":"browser_name",
                "browser_version":"browser_version"
            },
            "attributes":{
                "newName":"CountDistinct"
            }
        },
        "duration":10000
    }
    
### DISTINCT Aggregation

**BQL**

    SELECT browser_name AS browser
    FROM STREAM(30000, TIME)
    GROUP BY browser_name
    LIMIT 10;
    
**Bullet Query**

    {
        "aggregation":{
            "type":"GROUP",
            "size":10,
            "fields":{
                "browser_name":"browser"
            }
        },
        "duration":30000
    }
    
### GROUP BY Aggregation

**BQL**

    SELECT demographics.country AS country, device AS device,
           COUNT(*) AS count, AVG(demographics.age) AS averageAge,
           AVG(timespent) AS averageTimespent
    FROM STREAM(20000, TIME)
    WHERE demographics IS NOT NULL
    GROUP BY demographics.country, device
    LIMIT 50;

**Bullet Query**

    {
        "filters":[
            {
                "field":"demographics",
                "values":[
                    "NULL"
                ],
                "operation":"!="
            }
        ],
        "aggregation":{
            "type":"GROUP",
            "size":50,
            "fields":{
                "demographics.country":"country",
                "device":"device"
            },
            "attributes":{
                "operations":[
                    {
                        "newName":"count",
                        "type":"COUNT"
                    },
                    {
                        "newName":"averageTimespent",
                        "field":"timespent",
                        "type":"AVG"
                    },
                    {
                        "newName":"averageAge",
                        "field":"demographics.age",
                        "type":"AVG"
                    }
                ]
            }
        },
        "duration":20000
    }

### QUANTILE Distribution Aggregation

**BQL**

    SELECT QUANTILE(duration, LINEAR, 11)
    FROM STREAM(5000, TIME)
    LIMIT 11;
    
**Bullet Query**

    {
        "aggregation":{
            "type":"DISTRIBUTION",
            "size":11,
            "fields":{
                "duration":"duration"
            },
            "attributes":{
                "numberOfPoints":11,
                "type":"QUANTILE"
            }
        },
        "duration":5000
    }
    
### FREQ(Frequencies) Distribution Aggregation

**BQL**

    SELECT FREQ(duration, REGION, 2000, 20000, 500)
    FROM STREAM(5000, TIME)
    LIMIT 100;
    
**Bullet Query**

    {
        "aggregation":{
            "type":"DISTRIBUTION",
            "size":100,
            "fields":{
                "duration":"duration"
            },
            "attributes":{
                "start":2000.0,
                "increment":500.0,
                "end":20000.0,
                "type":"PMF"
            }
        },
        "duration":5000
    }
    
### CUMFREQ(Cumulative Frequencies) Distribution Aggregation

**BQL**
    
    SELECT CUMFREQ(duration, MANUAL, 20000, 2000, 15000, 45000)
    FROM STREAM(5000, TIME)
    LIMIT 100;
    
**Bullet Query**

    {
        "aggregation":{
            "type":"DISTRIBUTION",
            "size":100,
            "fields":{
                "duration":"duration"
            },
            "attributes":{
                "type":"CDF",
                "points":[
                    20000.0,2000.0,15000.0,45000.0
                ]
            }
        },
        "duration":5000
    }
    
### TOP K Aggregation

**BQL**

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
    
**Bullet Query**

    {
        "filters":[
            {
                "clauses":[
                    {
                        "field":"demographics.country",
                        "values":[
                            "NULL"
                        ],
                        "operation":"!="
                    },
                    {
                        "field":"browser_name",
                        "values":[
                            "NULL"
                        ],
                        "operation":"!="
                    }
                ],
                "operation":"AND"
            }
        ],
        "aggregation":{
            "type":"TOP K",
            "size":500,
            "fields":{
                "browser_name":"browser_name",
                "demographics.country":"demographics.country"
            },
            "attributes":{
                "newName":"numEvents",
                "threshold":100
            }
        },
        "duration":10000
    }

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
