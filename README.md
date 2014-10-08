# Light Swift

[Openstack Swift](http://docs.openstack.org/developer/swift/) API implementation in Node.js. 

## Installation

    $ npm install -g light-swift

## Quick Start

The quickest way to get started with light-swift is to utilize the executable `light-swift` to run the sample application using memory backend.

    $ light-swift

### MongoDB backend

    $ light-swift -a account -u username -k password -b mongo -t '["mongodb://localhost/lightswift"]' -s filesystem -o '["/tmp/lightswiftstorage"]'

### MySQL backend

    $ echo 'CREATE DATABASE `lightswift` DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;' | mysql -u root
    $ light-swift -a account -u username -k password -b mysql -t '["mysql://root:@localhost/lightswift"]' -s filesystem -o '["/tmp/lightswiftstorage"]'

### Filesystem backend

    $ light-swift -a account -u username -k password -b filesystem -t '["/tmp/lightswiftbackend"]' -s filesystem -o '["/tmp/lightswiftstorage"]'

## Development

    $ npm install

### Run tests

    $ MONGO_URL=mongodb://localhost/lightswifttest MYSQL_HOST=localhost MYSQL_USERNAME=root MYSQL_PASSWORD= MYSQL_DATABASE=lightswifttest cake test

### Coverage

    $ MONGO_URL=mongodb://localhost/lightswifttest MYSQL_HOST=localhost MYSQL_USERNAME=root MYSQL_PASSWORD= MYSQL_DATABASE=lightswifttest cake coverage

Open `coverage.html`

## Authors

Crafted by highly motivated engineers at [Koofr](http://koofr.net) and, hopefully, making your day just a little bit better.
