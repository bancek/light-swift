q = require('q')
path = require('path')
fs = require('fs')

{random32, Duplex} = require('../utils')

class FilesystemStorage
  constructor: (path) ->
    @path = path
    @objects = {}

  getPath: (object) ->
    path.join(@path, object)

  get: (object, range) =>
    filename = @getPath(object)

    q.nfcall(fs.stat, filename).then ->
      defer = q.defer()

      stream = fs.createReadStream(filename, range)

      stream.on 'open', ->
        defer.resolve(stream)

      stream.on 'error', (err) ->
        defer.reject(err)

      defer.promise

  create: (stream) =>
    defer = q.defer()

    name = random32()

    filename = @getPath(name)

    fileStream = fs.createWriteStream filename

    size = 0

    stream.pipe(fileStream)

    stream.on 'data', (data) ->
      size += data.length

    stream.on 'end', =>
      defer.resolve
        object: name
        size: size

    defer.promise

  remove: (object) =>
    filename = @getPath(object)

    q.nfcall(fs.unlink, filename)

module.exports = FilesystemStorage
