fs = require('fs')
os = require('os')
nodePath = require('path')
rimraf = require('rimraf')
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
