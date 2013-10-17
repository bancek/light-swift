# Light Swift

[Openstack Swift](http://docs.openstack.org/developer/swift/) API implementation in Node.js. 

## Installation

    $ npm install -g light-swift

## Quick Start

The quickest way to get started with light-swift is to utilize the executable `light-swift` to run the sample application.

    $ npm install -g light-swift
    $ light-swift

Usage help:

    $ light-swift --help

## Test coverage

    $ coffeeCoverage src lib # in light-swift
    $ mocha --compilers coffee:coffee-script --ignore-leaks --reporter html-cov > coverage.html # in swift-client

## Ensure MongoDB indices

```
db.users.ensureIndex({"_id.a": 1, "_id.u": 1})
db.containers.ensureIndex({"_id.a": 1})
db.containers.ensureIndex({"_id.a": 1, "_id.c": 1})
db.objects.ensureIndex({"_id.a": 1, "_id.c": 1})
db.objects.ensureIndex({"_id.a": 1, "_id.c": 1, "_id.o": 1})
```

## Authors

Crafted by highly motivated engineers at [Koofr](http://koofr.net) and, hopefully, making your day just a little bit better.
