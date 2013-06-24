stream = require('stream')
crypto = require('crypto')
_ = require('lodash')

random32 = ->
  _.range(32).map(-> (Math.random()*16 >> 0).toString(16)).join('')

dataStream = (data) ->
  stream = new Duplex()
  stream.write(data)
  stream.end()
  stream

class Duplex extends stream.Transform
  _transform: (chunk, encoding, cb) ->
    @push(chunk)
    cb()

class Hasher extends stream.Transform
  constructor: (options) ->
    super()
    @md5sum = crypto.createHash('md5')
    @md5sum.update('')
    @hash = null

  _transform: (chunk, encoding, done) =>
    @md5sum.update(chunk)
    @push(chunk)
    done()

  _flush: (done) =>
    @hash = @md5sum.digest('hex')
    done()

Hasher.hashArray = (arr) ->
  md5sum = crypto.createHash('md5')

  arr.forEach (txt) ->
    md5sum.update(txt)

  md5sum.digest('hex')

exports.random32 = random32
exports.dataStream = dataStream
exports.Duplex = Duplex
exports.Hasher = Hasher
