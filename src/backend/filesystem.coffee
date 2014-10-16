fs = require('fs')
q = require('q')
_ = require('lodash')
nodePath = require('path')
file = require('file')
lockFile = require('lockfile')
xattrAsync = require('xattr-async')

xattr =
  list: q.denodeify(xattrAsync.list)
  get: q.denodeify(xattrAsync.get)
  set: q.denodeify(xattrAsync.set)
  remove: q.denodeify(xattrAsync.remove)

if process.env.DEBUG
  calls = {}

  track = (name, func) ->
    inner = ->
      calls[name] += 1
      func.apply(func, arguments)

  for name of xattr
    calls[name] = 0
    xattr[name] = track name, xattr[name]

  setInterval ->
    console.log calls
  , 5000

ensureDirs = (path) ->
  q.ninvoke(file, 'mkdirs', path, null).fail((e) ->
    throw e if e?.code != 'EEXIST'
  )

ensureDir = (path) ->
  q.nfcall(fs.mkdir, path).fail((e) ->
    throw e if e?.code != 'EEXIST'
  )

class FilesystemBackend
  constructor: (basePath) ->
    @basePath = basePath or '/tmp/light-swift/'

  connect: =>
    ensureDirs(@basePath).then =>
      q.all [
        ensureDirs(nodePath.join(@basePath, 'data'))
        ensureDirs(nodePath.join(@basePath, 'auth'))
        ensureDirs(nodePath.join(@basePath, 'authtokens'))
      ]

  close: =>
    q()

  path: (args...) =>
    args.unshift(@basePath)
    nodePath.join.apply(nodePath.join, args)

  lock: (name, func) =>
    fileName = @path(name.replace(/\//g, '$').replace(/\.\./g, '$') + '.lock')

    opts =
      wait: 1000

    q.nfcall(lockFile.lock, fileName, opts).then ->
      func().fin ->
        q.nfcall(lockFile.unlock, fileName)

  xattrGetStr: (path, name, dest) =>
    xattr.get(path, 'user.' + name).then (data) ->
      dest[name] = data

  xattrSetStr: (path, name, src) =>
    xattr.set(path, 'user.' + name, src[name])

  xattrGetJson: (path, name) =>
    xattr.get(path, 'user.' + name).then (data) =>
      JSON.parse(data)

  xattrSetJson: (path, name, data) =>
    xattr.set(path, 'user.' + name, JSON.stringify(data))

  getMeta: (path) =>
    @xattrGetJson(path, 'meta')

  setMeta: (path, data) =>
    @lock path, =>
      @xattrSetJson(path, 'meta', data)

  updateMeta: (path, mutator) =>
    @lock path, =>
      @xattrGetJson(path, 'meta').then (data) =>
        mutator(data)
        @xattrSetJson(path, 'meta', data)

  # accounts

  accountPath: (account) =>
    @path('data', account)

  accountAuthPath: (account) =>
    @path('auth', account)

  addAccount: (account, accountInfo) =>
    path = @accountPath(account)
    authPath = @accountAuthPath(account)

    ensureDir(path).then =>
      ensureDir(authPath).then =>
        @setMeta path,
          bytesUsed: accountInfo.bytesUsed
          containerCount: accountInfo.containerCount
          objectCount: accountInfo.objectCount
          lastModified: accountInfo.lastModified.getTime()
          metadata: {}

  setAccountLastModified: (account, lastModified) =>
    path = @accountPath(account)

    @updateMeta path, (data) ->
      data.lastModified = lastModified.getTime()

  getAccount: (account) =>
    path = @accountPath(account)

    @getMeta(path).then((data) =>
      data.lastModified = new Date(data.lastModified)
      data
    , (err) =>
      null
    )

  setAccountMetadata: (account, metadata) =>
    path = @accountPath(account)

    @updateMeta path, (data) ->
      data.metadata = metadata

  accountAddContainer: (account) =>
    path = @accountPath(account)

    @updateMeta path, (data) ->
      data.containerCount += 1

  accountDeleteContainer: (account) =>
    path = @accountPath(account)

    @updateMeta path, (data) ->
      data.containerCount -= 1

  accountAddObject: (account, size) =>
    path = @accountPath(account)

    @updateMeta path, (data) ->
      data.objectCount += 1
      data.bytesUsed += size

  accountDeleteObject: (account, size) =>
    path = @accountPath(account)

    @updateMeta path, (data) ->
      data.objectCount -= 1
      data.bytesUsed -= size

  # auth

  userPath: (account, username) =>
    @path('auth', account, username)

  addUser: (account, username, key) =>
    path = @userPath(account, username)

    q.nfcall(fs.open, path, 'w').then (fd) =>
      q.nfcall(fs.close, fd).then =>
        @xattrSetStr(path, 'key', key: key)

  getUser: (account, username) =>
    path = @userPath(account, username)

    info = {}

    q.nfcall(fs.stat, path).then(=>
      @xattrGetStr(path, 'key', info).then =>
        info
    , (err) =>
      null
    )

  authTokenPath: (authToken) =>
    @path('authtokens', authToken)

  addAuthToken: (account, authToken) =>
    path = @authTokenPath(authToken)

    q.nfcall(fs.open, path, 'w').then (fd) =>
      q.nfcall(fs.close, fd).then =>
        @xattrSetStr(path, 'account', account: account)

  getAuthTokenAccount: (authToken) =>
    path = @authTokenPath(authToken)

    info = {}

    q.nfcall(fs.stat, path).then(=>
      @xattrGetStr(path, 'account', info).then =>
        info.account
    , (err) =>
      null
    )

  # containers

  containerPath: (account, container) =>
    @path('data', account, container)

  addContainer: (account, container, containerInfo) =>
    path = @containerPath(account, container)

    ensureDir(path).then =>
      @setMeta path,
        bytesUsed: containerInfo.bytesUsed
        objectCount: containerInfo.objectCount
        lastModified: containerInfo.lastModified.getTime()
        metadata: containerInfo.metadata
        acl:
          read: null
          write: null

  deleteContainer: (account, container) =>
    path = @containerPath(account, container)

    q.ninvoke(fs, 'rmdir', path)

  setContainerLastModified: (account, container, lastModified) =>
    path = @containerPath(account, container)

    @updateMeta path, (data) ->
      data.lastModified = lastModified.getTime()

  getContainer: (account, container) =>
    path = @containerPath(account, container)

    q.nfcall(fs.stat, path).then(=>
      @getMeta(path).then (data) =>
        data.lastModified = new Date(data.lastModified)
        data
    , (err) =>
      null
    )

  getContainers: (account) =>
    path = @accountPath(account)

    q.nfcall(fs.readdir, path).then (res) =>
      q.all(res.map((c) => @getContainer(account, c))).then (infos) ->
        _.zipObject(res, infos)

  setContainerMetadata: (account, container, metadata) =>
    path = @containerPath(account, container)

    @updateMeta path, (data) ->
      data.metadata = metadata

  setContainerAcl: (account, container, acl) =>
    path = @containerPath(account, container)

    @updateMeta path, (data) ->
      data.acl = acl

  containerAddObject: (account, container, size) =>
    path = @containerPath(account, container)

    @updateMeta path, (data) ->
      data.objectCount += 1
      data.bytesUsed += size

  containerDeleteObject: (account, container, size) =>
    path = @containerPath(account, container)

    @updateMeta path, (data) ->
      data.objectCount -= 1
      data.bytesUsed -= size

  # objects

  objectEscape: (object) =>
    new Buffer(object).toString('base64').replace(/\//, '.')

  objectUnescape: (escaped) =>
    new Buffer(escaped.replace(/\./, '/'), 'base64').toString()

  objectPath: (account, container, object) =>
    @path('data', account, container, @objectEscape(object))

  addObject: (account, container, object, obj) =>
    path = @objectPath(account, container, object)

    q.nfcall(fs.open, path, 'w').then (fd) =>
      q.nfcall(fs.close, fd).then =>
        @setMeta path,
          object: obj.object
          contentType: obj.contentType
          objectManifest: obj.objectManifest
          hash: obj.hash
          contentLength: obj.contentLength
          lastModified: obj.lastModified.getTime()
          metadata: obj.metadata

  setObjectLastModified: (account, container, object, lastModified) =>
    path = @objectPath(account, container, object)

    @updateMeta path, (data) ->
      data.lastModified = lastModified.getTime()

  deleteObject: (account, container, object) =>
    path = @objectPath(account, container, object)

    @getObject(account, container, object).then (obj) =>
      q.nfcall(fs.unlink, path).then ->
        obj

  getObject: (account, container, object) =>
    path = @objectPath(account, container, object)

    info =
      metadata: {}

    q.nfcall(fs.stat, path).then(=>
      @getMeta(path).then (data) =>
        data.lastModified = new Date(data.lastModified)
        data
    , (err) =>
      null
    )

  getObjects: (account, container) =>
    path = @containerPath(account, container)

    q.nfcall(fs.readdir, path).then (res) =>
      res = res.map(@objectUnescape)
      q.all(res.map((o) => @getObject(account, container, o))).then (infos) ->
        _.zipObject(res, infos)

  setObjectMetadata: (account, container, object, metadata) =>
    path = @objectPath(account, container, object)

    @updateMeta path, (data) ->
      data.metadata = metadata

module.exports = FilesystemBackend
