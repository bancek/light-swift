fs = require('fs')
q = require('q')
_ = require('lodash')
mime = require('mime')

MemoryStorage = require('./storage/memory')
MemoryBackend = require('./backend/memory')
SwiftServer = require('./server/swift')

{random32, dataStream, Hasher} = require('./utils')

defaultOptions =
  port: 3002
  verbose: no

class LightSwift
  BadHashError: class BadHashError extends Error
    constructor: -> super

  constructor: (opts) ->
    @options = _.merge({}, defaultOptions, opts)

    @storage = @options.storage or new MemoryStorage()
    @backend = @options.backend or new MemoryBackend()

  connect: =>
    @backend.connect()

  close: =>
    @_server?.close()
    @backend.close()

  # accounts

  addAccount: (account) =>
    @backend.getAccount(account).then (existing) =>
      return if existing?

      accountInfo =
        bytesUsed: 0
        containerCount: 0
        objectCount: 0
        metadata: {}
        lastModified: new Date()

      @backend.addAccount(account, accountInfo)

  bumpAccount: (account) =>
    @backend.setAccountLastModified(account, new Date())

  getAccount: (account) =>
    @backend.getAccount(account)

  mergeAccountMetadata: (account, newMetadata) =>
    @getAccount(account).then (accountInfo) =>
      metadata = _.merge({}, accountInfo.metadata, newMetadata)

      @backend.setAccountMetadata(account, metadata).then =>
        @bumpAccount(account)

  # auth

  addUser: (account, username, key) =>
    # TODO: use bcrypt

    @backend.getUser(account, username).then (existing) =>
      return if existing?

      @backend.addUser(account, username, key)

  newAuthToken: (account) =>
    authToken = 'AUTH_tk' + random32()

    @backend.addAuthToken(account, authToken).then =>
      authToken

  authenticate: (user, key) =>
    return q(no) if not user? or not key?

    parts = user.split(':')
    return q(no) if not parts.length == 2

    [account, username] = parts

    @backend.getUser(account, username).then (user) =>
      authenticated = user?.key == key

      authenticated

  getAuthTokenAccount: (authToken) =>
    @backend.getAuthTokenAccount(authToken)

  # containers

  addContainer: (account, container, metadata, acl) =>
    metadata = {} if not metadata?

    containerInfo =
      bytesUsed: 0
      objectCount: 0
      metadata: metadata
      acl: acl
      lastModified: new Date()

    @backend.getContainer(account, container).then (existing) =>
      return if existing?

      @backend.addContainer(account, container, containerInfo).then =>
        @backend.accountAddContainer(account)
          .then @bumpAccount(account)

  canDeleteContainer: (containerInfo) =>
    containerInfo.objectCount == 0

  deleteContainer: (account, container) =>
    @backend.deleteContainer(account, container).then =>
      @backend.accountDeleteContainer(account)
        .then @bumpAccount(account)

  bumpContainer: (account, container) =>
    @backend.setContainerLastModified(account, container, new Date())

  getContainer: (account, container) =>
    @backend.getContainer(account, container)

  getContainers: (account) =>
    @backend.getContainers(account)

  mergeContainerMetadata: (account, container, newMetadata) =>
    @getContainer(account, container).then (containerInfo) =>
      metadata = _.merge({}, containerInfo.metadata, newMetadata)

      @backend.setContainerMetadata(account, container, metadata).then =>
        @bumpContainer(account, container)

  mergeContainerAcl: (account, container, newAcl) =>
    @getContainer(account, container).then (containerInfo) =>
      acl = _.merge({}, containerInfo.acl, newAcl)

      @backend.setContainerAcl(account, container, acl).then =>
        @bumpContainer(account, container) # XXX: SHOULD WE BUMP HERE?

  # objects

  createObject: (account, container, object, obj, stream) =>
    hasher = new Hasher()
    stream = stream.pipe(hasher)

    @storage.create(stream).then (res) =>
      if obj.hash?
        if obj.hash != hasher.hash
          return @storage.remove(res.object).then =>
            throw new BadHashError("Bad hash")
      else
        obj.hash = hasher.hash

      obj.object = res.object
      obj.contentLength = res.size

      @addObject(account, container, object, obj)

  addObject: (account, container, object, obj) =>
    deleted = @deleteObject(account, container, object).fail(->)

    deleted.then =>
      obj.metadata = {} if not obj.metadata?
      obj.contentType = mime.lookup(object) if not obj.contentType?
      obj.lastModified = new Date() if not obj.lastModified?
      obj.hash = random32() if not obj.hash?

      @backend.addObject(account, container, object, obj).then =>
        @backend.accountAddObject(account, obj.contentLength)
          .then(@backend.containerAddObject(account, container, obj.contentLength))
          .then(@bumpAccount(account))
          .then(@bumpContainer(account, container))
          .then(-> obj)

  bumpObject: (account, container, object) =>
    @backend.setObjectLastModified(account, container, object, new Date())

  deleteObject: (account, container, object) =>
    @backend.deleteObject(account, container, object).then (obj) =>
      @storage.remove(obj.object).then =>
        @backend.accountDeleteObject(account, obj.contentLength)
          .then(@backend.containerDeleteObject(account, container, obj.contentLength))
          .then(@bumpAccount(account))
          .then(@bumpContainer(account, container))

  objectStream: (obj, range) =>
    @storage.get(obj.object, range)

  copyObject: (account, container, object, obj) =>
    @objectStream(obj).then (stream) =>
      @createObject(account, container, object, obj, stream)

  getObject: (account, container, object) =>
    @backend.getObject(account, container, object)

  getObjects: (account, container) =>
    @backend.getObjects(account, container)

  setObjectMetadata: (account, container, object, newMetadata) =>
    newMetadata = {} if not newMetadata?
    @backend.setObjectMetadata(account, container, object, newMetadata).then =>
      @bumpObject(account, container, object)

  importObjects: (account, container, objects) =>
    if not objects.length?
      objects = [objects]

    allCreated = objects.map (obj) =>
      stream = dataStream(obj.content)
      delete obj.content
      name = obj.name
      delete obj.name
      @createObject(account, container, name, obj, stream)

    q.all(allCreated)

  # http server

  server: =>
    @_server = new SwiftServer(@)
    @_server.listen()
    @options.port = @_server.httpServer.address().port;
    @_server

module.exports = LightSwift
