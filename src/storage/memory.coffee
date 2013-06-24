q = require('q')

{random32, Duplex} = require('../utils')

class MemoryStorage
  constructor: ->
    @objects = {}

  get: (object) =>
    buffer = @objects[object]

    if buffer?
      stream = new Duplex()
      stream.write(buffer)
      stream.end()
      q(stream)
    else
      q.reject(yes)

  create: (stream) =>
    defer = q.defer()

    name = random32()

    buffers = []

    stream.on 'data', (data) =>
      data = new Buffer(data)

      buffers.push(data)

    stream.on 'end', =>
      buffer = Buffer.concat(buffers)
      @objects[name] = buffer

      defer.resolve
        object: name
        size: buffer.length

    defer.promise

  remove: (object) =>
    delete @objects[object]

    q()

module.exports = MemoryStorage
