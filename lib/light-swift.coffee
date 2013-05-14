fs = require('fs')
http = require('http')
stream = require('stream')
express = require('express')
mime = require('mime')
_ = require('lodash')

defaultOptions =
  port: 3002
  tmpPath: null
  verbose: no
  putNull: no
  realName: no

class LightSwift
  constructor: (opts) ->
    @options = _.merge({}, defaultOptions, opts)

    if @options.tempDir and @options.tempDir.slice(-1) != '/'
      @options.tempDir += '/'

    @objectsStore = {}
    @accountsStore = {}
    @containersStore = {}

    @authTokensStore = {}

  addAccount: (account) =>
    @accountsStore[account] =
      users: {}
      bytesUsed: 0
      containerCount: 0
      objectCount: 0
      metadata: {}

    @containersStore[account] = {}

    @objectsStore[account] = {}

    @bumpAccount(account)

  bumpAccount: (account) =>
    @accountsStore[account].lastModified = new Date()

  bumpContainer: (account, container) =>
    @containersStore[account][container].lastModified = new Date()

  bumpObject: (account, container, object) =>
    @objectsStore[account][container][object].lastModified = new Date()

  addUser: (account, username, key) =>
    @accountsStore[account].users[username] =
      key: key

  addContainer: (account, container, metadata) =>
    @accountsStore[account].containerCount++

    metadata = {} if not metadata?

    @containersStore[account][container] =
      bytesUsed: 0
      objectCount: 0
      metadata: metadata

    @objectsStore[account][container] = {}

    @bumpContainer(account, container)

  deleteContainer: (account, container) =>
    info = @containersStore[account][container]
    delete @containersStore[account][container]

    delete @objectsStore[account][container]

    @accountsStore[account].containerCount--

    @bumpAccount(account)

  addObject: (account, container, object) =>
    if not object.length?
      object = [object]

    dest = @objectsStore[account][container]

    object.map (obj) =>
      if dest[obj.name]?
        @deleteObject(account, container, obj.name)

      if not obj.contentLength?
        if obj.filename
          obj.contentLength = fs.statSync(obj.filename).size
        else
          obj.contentLength = obj.content.length

      if not obj.contentType?
        obj.contentType = mime.lookup(obj.name)

      if not obj.metadata?
        obj.metadata = {}

      obj.lastModified = new Date() if not obj.lastModified?

      obj.hash = random32() if not obj.hash?

      @accountsStore[account].bytesUsed += obj.contentLength
      @accountsStore[account].objectCount++

      @containersStore[account][container].bytesUsed += obj.contentLength
      @containersStore[account][container].objectCount++

      dest[obj.name] = obj

  copyObject: (account, container, object, cb) =>
    stream = @getObjectStream(object)

    @createObject account, container, object, stream, cb

  deleteObject: (account, container, object) =>
    obj = @objectsStore[account][container][object]
    delete @objectsStore[account][container][object]

    @accountsStore[account].bytesUsed -= obj.contentLength
    @accountsStore[account].objectCount--

    @containersStore[account][container].bytesUsed -= obj.contentLength
    @containersStore[account][container].objectCount--

    @bumpAccount(account)
    @bumpContainer(account, container)

    if obj.filename?
      fs.unlink(obj.filename)

  newAuthToken: (account) =>
    authToken = "AUTH_tk" + random32()
    @authTokensStore[authToken] = account
    authToken

  authenticate: (user, key) =>
    return no if not user? or not key?
    parts = user.split(':')
    return no if not parts.length == 2
    [account, username] = parts
    @accountsStore[account]?.users[username]?.key == key

  createObject: (account, container, object, stream, cb) =>
    if @options.tempDir?
      if @options.realName
        object.filename = @options.tempDir + object.name.replace(/\//g, '-')
      else
        object.filename = @options.tempDir + random32()

      tmpFile = fs.createWriteStream object.filename
      
      size = 0

      stream.pipe(tmpFile)

      stream.on 'data', (data) ->
        size += data.length

      stream.on 'end', =>
        object.contentLength = size

        @addObject account, container, object

        cb(null, object)
    else
      reqData = ''

      stream.on 'data', (data) =>
        if not @options.putNull
          reqData += data

      stream.on 'end', =>
        object.content = reqData
        object.contentLength = object.content.length

        @addObject account, container, object

        cb(null, object)

  getObjectStream: (object) =>
    if object.filename?
      fs.createReadStream object.filename
    else
      stream = new Duplex()
      stream.write(object.content)
      stream.end()
      stream

  extractMetadata: (headers, type) =>
    type = 'object' if not type?
    regex = new RegExp("x-#{type}-meta-")
    removeRegex = new RegExp("x-remove-#{type}-meta-")

    md = _(headers).pairs()
      .filter(([k, v]) -> regex.test(k))
      .map(([k, v]) -> [k.replace(regex, ''), v])
      .object().value()

    _(headers).keys()
      .filter((k) -> removeRegex.test(k))
      .map((k) -> k.replace(removeRegex, ''))
      .forEach (k) ->
        md[k] = null

    md

  attachMetadata: (metadata, res, type) =>
    type = 'object' if not type?

    _(metadata).pairs().forEach ([k, v]) ->
      if v
        res.set "x-#{type}-meta-#{k}", v

  server: =>
    common = (req, res, next) =>
      if @options.verbose
        originalEnd = res.end
        
        res.end = (data) ->
          console.log req.method, req.url, res.statusCode

          originalEnd.call(res, data)

      res.lines = (list) =>
        res.set 'Content-Type', 'text/plain; charset=utf-8'
        res.send list.map((x) -> x + '\n').join('')

      # res.set 'Accept-Ranges', 'bytes'
      res.set 'X-Trans-Id', 'tx' + random32()

      res.timestamp = (date) =>
        date = new Date() if not date?
        res.set 'X-Timestamp', date.getTime() / 1000

      req.json = req.get('Accept') == 'application/json'\
        or req.get('Content-Type') == 'application/json'\
        or req.get('Accept') == 'text/json'\
        or req.get('Content-Type') == 'text/json'\
        or req.param('format') == 'json'

      req.head = req.method == 'HEAD'

      authToken = req.get('x-auth-token')

      if @authTokensStore[authToken]?
        req.account = @authTokensStore[authToken]
        
        if req.account == req.param('account')
          req.accountInfo = @accountsStore[req.account]
          req.containers = @objectsStore[req.account]
          req.containerInfos = @containersStore[req.account]

          req.container = req.param('container')

          if req.container
            req.containerInfo = @containersStore[req.account][req.container]
            req.objects = req.containers[req.container]

            req.object = req.param(0)

          next()
        else
          res.send 403
      else
        res.send 401

    app = express()

    app.configure =>
      app.disable('x-powered-by')
      app.use(app.router)

    app.get '/auth/v1.0*', (req, res) =>
      user = req.get('x-auth-user')
      key = req.get('x-auth-key')

      if @authenticate(user, key)
        account = user.split(':')[0]
        authToken = @newAuthToken(account)

        host = req.get('host')

        res.set 'X-Storage-Url', "http://#{host}/v1/AUTH_#{account}"
        res.set 'X-Auth-Token', authToken
        res.set 'X-Storage-Token', authToken

        res.send 200
      else
        res.send 401

    app.get '/v1/AUTH_:account', common, (req, res) =>
      containers = _.pairs(req.containerInfos).map ([name, info]) ->
        name: name
        count: info.objectCount
        bytes: info.bytesUsed

      res.set 'X-Account-Bytes-Used', req.accountInfo.bytesUsed
      res.set 'X-Account-Container-Count', req.accountInfo.containerCount
      res.set 'X-Account-Object-Count', req.accountInfo.objectCount

      res.timestamp(req.accountInfo.lastModified)

      @attachMetadata(req.accountInfo.metadata, res, 'account')

      if req.param('marker')?
        containers = []

      if req.head
        return res.send(204)

      if req.json
        res.json containers
      else
        res.lines containers.map((x) -> x.name)

    app.post '/v1/AUTH_:account', common, (req, res) =>
      md = @extractMetadata(req.headers, 'account')
      req.accountInfo.metadata = _.merge(req.accountInfo.metadata, md)

      @bumpAccount(req.account)

      res.send 204

    app.put '/v1/AUTH_:account', common, (req, res) =>
      res.send 403

    app.delete '/v1/AUTH_:account', common, (req, res) =>
      res.send 403

    app.get '/v1/AUTH_:account/:container', common, (req, res) =>
      objects = req.objects

      if not objects?
        return res.send 404

      objs = _.keys objects

      res.set 'X-Container-Bytes-Used', req.containerInfo.bytesUsed
      res.set 'X-Container-Object-Count', req.containerInfo.objectCount

      res.timestamp(req.containerInfo.lastModified)

      @attachMetadata(req.containerInfo.metadata, res, 'container')

      if req.head
        return res.send(204)

      prefix = req.param('prefix')
      delimiter = req.param('delimiter')
      path = req.param('path')
      marker = req.param('marker')

      pathMode = no

      if path
        path = path + '/' if path.slice(-1) != '/'

        prefix = path
        delimiter = '/'
        pathMode = yes

      if prefix
        prefixRegex = new RegExp('^' + prefix)

        objs = _.filter objs, (obj) => prefixRegex.test(obj)

      if delimiter
        pfx = prefix or ''

        objs = _.map objs, (obj) =>
          obj = obj.substring(pfx.length)
          parts = obj.split(delimiter)

          if parts.length == 1
            pfx + parts[0]
          else
            pfx + parts[0] + delimiter

        objs = _.uniq objs

      objsMeta = objs.map (objName) =>
        obj = objects[objName]

        if obj
          bytes: obj.contentLength,
          content_type: obj.contentType,
          hash: obj.hash,
          last_modified: obj.lastModified.toISOString().slice(0, -1),
          name: objName
        else
          subdir: objName

      if pathMode
        objsMeta = objsMeta.filter (x) -> not x.subdir?

      if marker?
        objsMeta = []

      if req.json
        res.json objsMeta
      else
        if objsMeta.length == 0
          res.statusCode = 204

        res.lines _.map(objsMeta, (x) -> x.name or x.subdir)

    app.put '/v1/AUTH_:account/:container', common, (req, res) =>
      if not req.containerInfo?
        md = @extractMetadata(req.headers, 'container')
        @addContainer(req.account, req.container, md)
        res.send 201
      else
        res.send 202

    app.post '/v1/AUTH_:account/:container', common, (req, res) =>
      if req.containerInfo?
        md = @extractMetadata(req.headers, 'container')
        req.containerInfo.metadata = _.merge(req.containerInfo.metadata, md)

        @bumpContainer(req.account, req.container)

        res.send 204
      else
        res.send 404

    app.delete '/v1/AUTH_:account/:container', common, (req, res) =>
      if req.containerInfo?
        if req.containerInfo.objectCount == 0
          @deleteContainer(req.account, req.container)
          res.send 204
        else
          res.send 409
      else
        res.send 404

    app.get '/v1/AUTH_:account/:container/*', common, (req, res) =>
      objects = req.objects
      obj = objects[req.object]

      if obj?
        res.set 'Content-type', obj.contentType
        res.set 'Last-Modified', obj.lastModified.toUTCString()
        res.set 'Etag', obj.hash

        res.timestamp(obj.lastModified)

        @attachMetadata(obj.metadata, res)

        segments = null

        if obj.objectManifest
          res.set 'X-Object-Manifest', obj.objectManifest

          parts = obj.objectManifest.split('/')
          container = parts.shift()
          manifest = parts.join('/')

          regex = new RegExp('^' + manifest)

          segments = _.pairs(objects).filter(([name, seg]) -> regex.test(name)).map(([name, seg]) -> seg)
        else
          segments = [obj]

        contentLength = segments.map((x) -> x.contentLength).reduce(((x, y) -> x + y), 0)

        res.set 'Content-Length', contentLength

        if req.head
          return res.end()
        
        next = =>
          segment = segments.shift()

          if segment?
            stream = @getObjectStream(segment)

            stream.pipe(res, end: no)

            stream.on 'end', ->
              stream.unpipe(res)

              next()
          else
            res.end()
        
        next()
      else
        res.send 404

    parseCopyPath = (path) =>
      regex = new RegExp('/?([^/]+)/(.+)')

      parts = path.match(regex)

      return if not parts

      container: parts[1]
      object: parts[2]

    app.put '/v1/AUTH_:account/:container/*', common, (req, res) =>
      getCopyFrom = ->
        return if req.get('content-length') != '0'

        copyFrom = req.get('x-copy-from')

        return if not copyFrom

        return parseCopyPath(copyFrom)

      copyInfo = getCopyFrom()

      metadata = @extractMetadata(req.headers)

      if copyInfo?
        obj = req.containers[copyInfo.container]?[copyInfo.object]

        if not obj?
          return res.send 404

        obj = _.cloneDeep(obj)
        obj.name = req.object

        if req.get('content-type')
          obj.contentType = req.get('content-type')

        if req.get('x-object-manifest')
          obj.objectManifest = req.get('x-object-manifest')

        if _.keys(metadata).length
          obj.metadata = metadata

        return @copyObject req.account, req.container, obj, (err, obj) =>
          res.set 'Etag', obj.hash

          res.send 201

      obj =
        name: req.object
        contentType: req.get('content-type')
        objectManifest: req.get('x-object-manifest')

      if _.keys(metadata).length
          obj.metadata = metadata

      @createObject req.account, req.container, obj, req, (err, obj) =>
        res.set 'Etag', obj.hash

        res.send 201

    app.copy '/v1/AUTH_:account/:container/*', common, (req, res) =>
      return res.send 412 if not req.get('destination')

      copyInfo = parseCopyPath(req.get('destination'))

      return res.send 412 if not copyInfo?

      obj = req.objects[req.object]

      return res.send 404 if not obj?

      metadata = @extractMetadata(req.headers)

      obj = _.cloneDeep(obj)
      obj.name = copyInfo.object

      if req.get('content-type')
        obj.contentType = req.get('content-type')

      if req.get('x-object-manifest')
        obj.objectManifest = req.get('x-object-manifest')

      if _.keys(metadata).length
        obj.metadata = metadata

      @copyObject req.account, copyInfo.container, obj, (err) =>
        res.send 201

    app.post '/v1/AUTH_:account/:container/*', common, (req, res) =>
      obj = req.objects[req.object]
      obj.metadata = @extractMetadata(req.headers)

      @bumpObject(req.account, req.container, req.object)
      res.send 202

    app.delete '/v1/AUTH_:account/:container/*', common, (req, res) =>
      if req.objects[req.object]?
        @deleteObject(req.account, req.container, req.object)
        res.send 204
      else
        res.send 404

    @httpServer = http.createServer(app)

    @httpServer.listen(@options.port)

class Duplex extends stream.Transform
  _transform: (chunk, encoding, cb) ->
    @push(chunk)
    cb()

random32 = ->
  _.range(32).map(-> (Math.random()*16 >> 0).toString(16)).join('')

module.exports = LightSwift
