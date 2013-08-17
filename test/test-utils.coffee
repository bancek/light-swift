q = require('q')

exports.consume = (stream) ->
  defer = q.defer()

  content = ''

  if stream.closed
    defer.resolve(content)
  else
    stream.on 'data', (data) ->
      content += data

    stream.on 'end', ->
      defer.resolve(content)

  defer.promise
