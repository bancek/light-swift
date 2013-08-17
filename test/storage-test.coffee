fs = require('fs')
os = require('os')
nodePath = require('path')
rimraf = require('rimraf')
q = require('q')
should = require('chai').should()
require('mocha-as-promised')()

MemoryStorage = require('../src/storage/memory')
FilesystemStorage = require('../src/storage/filesystem')

{dataStream} = require('../src/utils')
{consume} = require('./test-utils')

[
  ['MemoryStorage', (-> [new MemoryStorage(), (->)])]
  ['FilesystemStorage', (->
    path = nodePath.join(os.tmpDir(), 'FilesystemStorage/')

    cleanup = -> rimraf.sync(path)

    try cleanup()

    fs.mkdirSync(path)

    [new FilesystemStorage(path), cleanup]
  )]
].forEach ([name, getStorage]) ->
  describe name, ->
    storage = null
    cleanup = null

    beforeEach ->
      [storage, cleanup] = getStorage()

    afterEach ->
      storage = null
      cleanup()

    it 'should create object', ->
      stream = dataStream('12345')

      storage.create(stream).then (res) ->
        should.exist res.object
        res.size.should.equal 5

    it 'should get object', ->
      stream = dataStream('12345')

      storage.create(stream).then (res) ->
        object = res.object

        storage.get(object).then (stream) ->
          should.exist stream

          consume(stream).then (content) ->
            content.should.equal '12345'

    it 'should get object with range', ->
      stream = dataStream('12345')

      storage.create(stream).then (res) ->
        object = res.object

        range = (start, end, eq) ->
          storage.get(object, start: start, end: end).then (stream) ->
            consume(stream).then (content) ->
              content.should.equal eq

        q.all [
          range(0, 0, '1')
          range(1, 1, '2')
          range(2, 2, '3')
          range(3, 3, '4')
          range(4, 4, '5')
          range(0, 4, '12345')
          range(1, 4, '2345')
          range(1, 3, '234')
        ]

    it 'should create and get empty object', ->
      stream = dataStream('')

      storage.create(stream).then (res) ->
        res.size.should.equal 0

        object = res.object

        storage.get(object).then (stream) ->
          should.exist stream

          consume(stream).then (content) ->
            content.should.equal ''

    it 'should remove object', ->
      stream = dataStream('12345')

      storage.create(stream).then (res) ->
        object = res.object

        storage.remove(object).then ->
          storage.get(object).fail (err) ->
            should.exist err
