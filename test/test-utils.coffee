q = require('q')

exports.consume = (stream) ->
  defer = q.defer()

  content = ''

  stream.on 'data', (data) ->
    content += data

  stream.on 'end', ->
    defer.resolve(content)

  defer.promise
