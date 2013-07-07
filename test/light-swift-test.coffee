q = require('q')
_ = require('lodash')
should = require('chai').should()
require('mocha-as-promised')()

LightSwift = require('../src/light-swift')

describe 'Light Swift', ->
  account = 'test'
  username = 'tester'
  key = 'testing'

  describe 'import objects', ->
    swift = null

    beforeEach (done) ->
      swift = new LightSwift()

      swift.connect()
        .then(-> swift.addAccount(account))
        .then(-> swift.addUser(account, username, key))
        .then(-> done())

    afterEach (done) ->
      swift.close().then(-> done())

    it 'should import objects', ->
      object =
        name: 'file.txt'
        contentType: 'text/plain'
        lastModified: new Date('2013-04-22T16:58:36.698Z')
        hash: '827ccb0eea8a706c4c34a16891f84e7b'
        content: '12345'

      objects = [
        name: 'dir1/file1.txt'
        contentType: 'text/plain'
        lastModified: new Date('2013-04-22T16:58:36.698Z')
        hash: 'a09ebcef8ab11daef0e33e4394ea775f'
        content: 'dir1/file1'
      ,
        name: 'dir1/file2.txt'
        contentType: 'text/plain'
        lastModified: new Date('2013-04-22T16:58:36.698Z')
        hash: '725d747aeea47a4d8f6432ef5a9ee268'
        content: 'dir1/file2'
      ]

      swift.addContainer(account, 'import-test').then ->
        swift.importObjects(account, 'import-test', object).then ->
          swift.importObjects(account, 'import-test', objects).then ->
            swift.getObjects(account, 'import-test').then (objects) ->
              _.forEach objects, (x) ->
                delete x.object

              objects.should.eql
                'file.txt':
                  contentType: 'text/plain'
                  lastModified: new Date('2013-04-22T16:58:36.698Z')
                  hash: '827ccb0eea8a706c4c34a16891f84e7b'
                  contentLength: 5
                  metadata: {}

                'dir1/file1.txt':
                  contentType: 'text/plain'
                  lastModified: new Date('2013-04-22T16:58:36.698Z')
                  hash: 'a09ebcef8ab11daef0e33e4394ea775f'
                  contentLength: 10
                  metadata: {}

                'dir1/file2.txt':
                  contentType: 'text/plain'
                  lastModified: new Date('2013-04-22T16:58:36.698Z')
                  hash: '725d747aeea47a4d8f6432ef5a9ee268'
                  contentLength: 10
                  metadata: {}
