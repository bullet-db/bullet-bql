# Bullet-BQL

[![Build Status](https://cd.screwdriver.cd/pipelines/7222/badge)](https://cd.screwdriver.cd/pipelines/7222)
[![Coverage Status](https://coveralls.io/repos/github/bullet-db/bullet-bql/badge.svg?branch=master)](https://coveralls.io/github/bullet-db/bullet-bql?branch=master) 
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.yahoo.bullet/bullet-bql/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.yahoo.bullet/bullet-bql/)

>BQL is a SQL-like query language specifically designed for the [Bullet](https://bullet-db.github.io/) query engine, which provides an easy-to-use yet powerful interactive SQL-like interface. 

This project contains a BQL parser built in [ANTLR 4](http://www.antlr.org/). A BQL query will be parsed, classified, validated and extracted to a [Bullet](https://bullet-db.github.io/) Query.

## Table of Contents

- [Background](#background)
- [Install](#install)
- [Usage](#usage)
- [Documentation](#documentation)
- [Useful Links](#useful-links)
- [Contribute](#contribute)
- [License](#license)
    
## Background

Bullet-BQL is created to provide users with a friendly SQL-like layer to manipulate the power of [Bullet](https://bullet-db.github.io/) query engine.

## Install

* The Bullet-BQL artifact can be obtained from [JCenter](https://bintray.com/bintray/jcenter?filterByPkgName=bullet-bql).

* To run the tool from the command line, first compile:

    `mvn clean compile` 

    Then you can use bullet-bql to parse BQL strings into [Bullet JSON](https://bullet-db.github.io/ws/api/) queries. To run:
    
    `mvn exec:java`
     
* Bullet-BQL is currently being integrated into [Bullet-Service](https://github.com/bullet-db/bullet-service/), and will provide a BQL endpoint directly. 

## Usage

* Build a [Bullet](https://bullet-db.github.io/) Query from a BQL string.

    Simply construct a `BulletQueryBuilder` and call `buildQuery(String bql)`. A [Bullet](https://bullet-db.github.io/) Query is returned.

* You can change the max query length in `BQLConfig` by altering the `.yaml`.

## Documentation

- [BQL - Bullet Docs](https://bullet-db.github.io/ws/api/) to see the BQL grammar.
- [Examples - Bullet Docs](https://bullet-db.github.io/ws/examples/) to see BQL query examples.

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
