q = require('q')
_ = require('lodash')
mongodb = require('mongodb')

class MongoBackend
  constructor: (url) ->
    @databaseUrl = url or 'mongodb://localhost/lightswift'

  connect: =>
    collections = [
      'accounts',
      'users',
      'authTokens',
      'containers',
      'objects'
    ]

    q.ninvoke(mongodb, 'connect', @databaseUrl).then (db) =>
      @db = db

      allCollections = collections.map (name) =>
        q.ninvoke(db, 'collection', name).then (col) =>
          @[name] = col

      q.all(allCollections)

  close: =>
    return q() if not @db?
    q.ninvoke(@db, 'close')

  # accounts

  addAccount: (account, accountInfo) =>
    accountInfo._id = account
    q.ninvoke(@accounts, 'save', accountInfo)

  setAccountLastModified: (account, lastModified) =>
    qry = _id: account
    data = $set: {lastModified: lastModified}
    q.ninvoke(@accounts, 'update', qry, data)

  getAccount: (account) =>
    qry = _id: account
    q.ninvoke(@accounts, 'findOne', qry).then (a) =>
      delete a?._id
      a

  setAccountMetadata: (account, metadata) =>
    qry = _id: account
    data = $set: {metadata: metadata}
    q.ninvoke(@accounts, 'update', qry, data)

  accountAddContainer: (account) =>
    qry = _id: account
    data = $inc: {containerCount: 1}
    q.ninvoke(@accounts, 'update', qry, data)

  accountDeleteContainer: (account) =>
    qry = _id: account
    data = $inc: {containerCount: -1}
    q.ninvoke(@accounts, 'update', qry, data)

  accountAddObject: (account, size) =>
    qry = _id: account
    data = $inc:
      objectCount: 1
      bytesUsed: size

    q.ninvoke(@accounts, 'update', qry, data)

  accountDeleteObject: (account, size) =>
    qry = _id: account
    data = $inc:
      objectCount: -1
      bytesUsed: -size

    q.ninvoke(@accounts, 'update', qry, data)

  # auth

  addUser: (account, username, key) =>
    user =
      _id: {a: account, u: username}
      key: key

    q.ninvoke(@users, 'save', user)

  getUser: (account, username) =>
    qry = _id: {a: account, u: username}

    q.ninvoke(@users, 'findOne', qry)

  addAuthToken: (account, authToken) =>
    data =
      _id: authToken
      account: account

    q.ninvoke(@authTokens, 'save', data)

  getAuthTokenAccount: (authToken) =>
    qry = _id: authToken
    q.ninvoke(@authTokens, 'findOne', qry).then (data) ->
      data?.account

  # containers

  addContainer: (account, container, containerInfo) =>
    containerInfo._id = {a: account, c: container}
    q.ninvoke(@containers, 'save', containerInfo)

  deleteContainer: (account, container) =>
    qry = _id: {a: account, c: container}
    q.ninvoke(@containers, 'remove', qry)

  setContainerLastModified: (account, container, lastModified) =>
    qry = _id: {a: account, c: container}
    data = $set: {lastModified: lastModified}
    q.ninvoke(@containers, 'update', qry, data)

  getContainer: (account, container) =>
    qry = _id: {a: account, c: container}
    q.ninvoke(@containers, 'findOne', qry).then (c) ->
      delete c?._id
      c

  getContainers: (account) =>
    qry = {'_id.a': account}
    cur = @containers.find(qry)

    q.ninvoke(cur, 'toArray').then (res) =>
      _(res)
        .map((x) -> [x._id.c, x])
        .map(([k, v]) -> delete v._id; [k, v])
        .object()
        .value()

  setContainerMetadata: (account, container, metadata) =>
    qry = _id: {a: account, c: container}
    data = $set: {metadata: metadata}
    q.ninvoke(@containers, 'update', qry, data)

  setContainerAcl: (account, container, acl) =>
    qry = _id: {a: account, c: container}
    data = $set: {acl: acl}
    q.ninvoke(@containers, 'update', qry, data)

  containerAddObject: (account, container, size) =>
    qry = _id: {a: account, c: container}
    data = $inc:
      objectCount: 1
      bytesUsed: size

    q.ninvoke(@containers, 'update', qry, data)

  containerDeleteObject: (account, container, size) =>
    qry = _id: {a: account, c: container}
    data = $inc:
      objectCount: -1
      bytesUsed: -size

    q.ninvoke(@containers, 'update', qry, data)

  # objects

  addObject: (account, container, obj) =>
    obj._id = a: account, c: container, o: obj.name
    q.ninvoke(@objects, 'save', obj)

  setObjectLastModified: (account, container, object, lastModified) =>
    qry = _id: {a: account, c: container, o: object}
    data = $set: {lastModified: lastModified}
    q.ninvoke(@objects, 'update', qry, data)

  deleteObject: (account, container, object) =>
    @getObject(account, container, object).then (obj) =>
      qry = _id: {a: account, c: container, o: object}
      q.ninvoke(@objects, 'remove', qry).then =>
        obj

  getObject: (account, container, object) =>
    qry = _id: {a: account, c: container, o: object}
    q.ninvoke(@objects, 'findOne', qry).then (o) ->
      delete o?._id
      o

  getObjects: (account, container) =>
    qry = {'_id.a': account, '_id.c': container}
    cur = @objects.find(qry)

    q.ninvoke(cur, 'toArray').then (res) =>
      _(res)
        .map((x) -> [x._id.o, x])
        .map(([k, v]) -> delete v._id; [k, v])
        .object()
        .value()

  setObjectMetadata: (account, container, object, metadata) =>
    qry = _id: {a: account, c: container, o: object}
    data = $set: {metadata: metadata}
    q.ninvoke(@objects, 'update', qry, data)

module.exports = MongoBackend
