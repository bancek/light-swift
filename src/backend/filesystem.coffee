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
      ensureDirs(nodePath.join(@basePath, 'data')).then =>
        ensureDirs(nodePath.join(@basePath, 'auth')).then =>
          ensureDirs(nodePath.join(@basePath, 'authtokens'))

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

  touch: (path, mtime) =>
    q.nfcall(fs.utimes, path, mtime, mtime)

  xattrGetInt: (path, name, dest) =>
    xattr.get(path, 'user.' + name).then (data) ->
      dest[name] = data - 0

  xattrSetInt: (path, name, src) =>
    xattr.set(path, 'user.' + name, '' + src[name])

  xattrIncrInt: (path, name, amount) =>
    key = 'user.' + name

    @lock path, =>
      xattr.get(path, key).then (data) =>
        xattr.set(path, key, '' + ((data - 0) + amount))

  xattrGetStr: (path, name, dest) =>
    xattr.get(path, 'user.' + name).then (data) ->
      dest[name] = data

  xattrSetStr: (path, name, src) =>
    xattr.set(path, 'user.' + name, src[name])

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
        q.all([
          @xattrSetInt(path, 'bytesUsed', accountInfo)
          @xattrSetInt(path, 'containerCount', accountInfo)
          @xattrSetInt(path, 'objectCount', accountInfo)
          @setAccountLastModified(account, accountInfo.lastModified)
        ])

  setAccountLastModified: (account, lastModified) =>
    path = @accountPath(account)
    @xattrSetInt(path, 'lastModified', lastModified: lastModified.getTime())

  getAccount: (account) =>
    path = @accountPath(account)

    info =
      metadata: {}

    q.all([
      @xattrGetInt(path, 'lastModified', info).then(-> info.lastModified = new Date(info.lastModified))
      @xattrGetInt(path, 'bytesUsed', info)
      @xattrGetInt(path, 'containerCount', info)
      @xattrGetInt(path, 'objectCount', info)
      xattr.list(path).then((attrs) =>
        mattrs = attrs.filter((attr) -> /^user\.meta\./.test(attr))

        promises = mattrs.map (attr) =>
          xattr.get(path, attr).then (val) ->
            key = attr.replace(/^user\.meta\./, '')
            info.metadata[key] = val

        q.all(promises)
      )
    ]).then =>
      info

  setAccountMetadata: (account, metadata) =>
    path = @accountPath(account)

    xattr.list(path).then((attrs) =>
      mattrs = attrs.filter((attr) -> /^user\.meta\./.test(attr))
      q.all(mattrs.map (attr) => xattr.remove(path, attr))
    ).then(=>
      q.all(_.keys(metadata).map((key) => xattr.set(path, 'user.meta.' + key, metadata[key])))
    )

  accountAddContainer: (account) =>
    path = @accountPath(account)
    @xattrIncrInt(path, 'containerCount', 1)

  accountDeleteContainer: (account) =>
    path = @accountPath(account)
    @xattrIncrInt(path, 'containerCount', -1)

  accountAddObject: (account, size) =>
    path = @accountPath(account)

    @xattrIncrInt(path, 'objectCount', 1).then =>
      @xattrIncrInt(path, 'bytesUsed', size)

  accountDeleteObject: (account, size) =>
    path = @accountPath(account)

    @xattrIncrInt(path, 'objectCount', -1).then =>
      @xattrIncrInt(path, 'bytesUsed', -size)

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
      q.all([
        @xattrSetInt(path, 'bytesUsed', containerInfo)
        @xattrSetInt(path, 'objectCount', containerInfo)
        @setContainerLastModified(account, container, containerInfo.lastModified)
        @setContainerMetadata(account, container, containerInfo.metadata)
      ])

  deleteContainer: (account, container) =>
    path = @containerPath(account, container)

    q.ninvoke(fs, 'rmdir', path)

  setContainerLastModified: (account, container, lastModified) =>
    path = @containerPath(account, container)

    @xattrSetInt(path, 'lastModified', lastModified: lastModified.getTime())

  getContainer: (account, container) =>
    path = @containerPath(account, container)

    info =
      metadata: {}
      acl:
        read: null
        write: null

    q.nfcall(fs.stat, path).then(=>
      q.all([
        @xattrGetInt(path, 'lastModified', info).then(-> info.lastModified = new Date(info.lastModified))
        @xattrGetInt(path, 'bytesUsed', info)
        @xattrGetInt(path, 'objectCount', info)
        xattr.list(path).then((attrs) =>
          mattrs = attrs.filter((attr) -> /^user\.meta\./.test(attr))

          promises = mattrs.map (attr) =>
            xattr.get(path, attr).then (val) ->
              key = attr.replace(/^user\.meta\./, '')
              info.metadata[key] = val

          q.all(promises)
        )
        xattr.get(path, 'user.acl.read').fail(->).then((val) =>
          info.acl.read = val or null
        )
        xattr.get(path, 'user.acl.write').fail(->).then((val) =>
          info.acl.write = val or null
        )
      ]).then =>
        info
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

    xattr.list(path).then((attrs) =>
      mattrs = attrs.filter((attr) -> /^user\.meta\./.test(attr))
      q.all(mattrs.map (attr) => xattr.remove(path, attr))
    ).then(=>
      q.all(_.keys(metadata).map((key) => xattr.set(path, 'user.meta.' + key, metadata[key])))
    )

  setContainerAcl: (account, container, acl) =>
    path = @containerPath(account, container)

    q.all([
      xattr.set(path, 'user.acl.read', acl.read or '')
      xattr.set(path, 'user.acl.write', acl.write or '')
    ])

  containerAddObject: (account, container, size) =>
    path = @containerPath(account, container)

    @xattrIncrInt(path, 'objectCount', 1).then =>
      @xattrIncrInt(path, 'bytesUsed', size)

  containerDeleteObject: (account, container, size) =>
    path = @containerPath(account, container)

    @xattrIncrInt(path, 'objectCount', -1).then =>
      @xattrIncrInt(path, 'bytesUsed', -size)

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
        q.all([
          @xattrSetStr(path, 'object', obj)
          @xattrSetStr(path, 'contentType', obj)
          @xattrSetStr(path, 'hash', obj)
          @xattrSetInt(path, 'contentLength', obj)
          @setObjectLastModified(account, container, object, obj.lastModified)
          @setObjectMetadata(account, container, object, obj.metadata)
        ])

  setObjectLastModified: (account, container, object, lastModified) =>
    path = @objectPath(account, container, object)

    @xattrSetInt(path, 'lastModified', lastModified: lastModified.getTime())

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
      q.all([
        @xattrGetStr(path, 'object', info)
        @xattrGetStr(path, 'contentType', info)
        @xattrGetStr(path, 'hash', info)
        @xattrGetInt(path, 'contentLength', info)
        @xattrGetInt(path, 'lastModified', info).then(-> info.lastModified = new Date(info.lastModified))
        xattr.list(path).then((attrs) =>
          mattrs = attrs.filter((attr) -> /^user\.meta\./.test(attr))

          promises = mattrs.map (attr) =>
            xattr.get(path, attr).then (val) ->
              key = attr.replace(/^user\.meta\./, '')
              info.metadata[key] = val

          q.all(promises)
        )
      ]).then =>
        info
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

    xattr.list(path).then((attrs) =>
      mattrs = attrs.filter((attr) -> /^user\.meta\./.test(attr))
      q.all(mattrs.map (attr) => xattr.remove(path, attr))
    ).then(=>
      q.all(_.keys(metadata).map((key) => xattr.set(path, 'user.meta.' + key, metadata[key])))
    )

module.exports = FilesystemBackend
