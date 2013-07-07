q = require('q')
_ = require('lodash')

class MemoryBackend
  constructor: ->
    @accountsStore = {}
    @usersStore = {}
    @authTokensStore = {}
    @containersStore = {}
    @objectsStore = {}

  connect: ->
    q()

  close: ->
    q()

  # accounts

  addAccount: (account, accountInfo) =>
    @accountsStore[account] = accountInfo
    @usersStore[account] = {}
    @containersStore[account] = {}
    @objectsStore[account] = {}

    q()

  setAccountLastModified: (account, lastModified) =>
    @accountsStore[account].lastModified = lastModified
    q()

  getAccount: (account) =>
    accountInfo = @accountsStore[account]
    q(accountInfo)

  setAccountMetadata: (account, metadata) =>
    @getAccount(account).then (accountInfo) =>
      accountInfo.metadata = metadata

  accountAddContainer: (account) =>
    @accountsStore[account].containerCount++
    q()

  accountDeleteContainer: (account) =>
    @accountsStore[account].containerCount--
    q()

  accountAddObject: (account, size) =>
    @accountsStore[account].objectCount++
    @accountsStore[account].bytesUsed += size
    q()

  accountDeleteObject: (account, size) =>
    @accountsStore[account].objectCount--
    @accountsStore[account].bytesUsed -= size
    q()

  # auth

  addUser: (account, username, key) =>
    @usersStore[account][username] =
      key: key
    q()

  getUser: (account, username) =>
    user = @usersStore[account]?[username]
    q(user)

  addAuthToken: (account, authToken) =>
    @authTokensStore[authToken] = account
    q()

  getAuthTokenAccount: (authToken) =>
    account = @authTokensStore[authToken]
    q(account)

  # containers

  addContainer: (account, container, containerInfo) =>
    @containersStore[account][container] = containerInfo
    @objectsStore[account][container] = {}
    q()

  deleteContainer: (account, container) =>
    delete @objectsStore[account][container]
    delete @containersStore[account][container]
    q()

  setContainerLastModified: (account, container, lastModified) =>
    @containersStore[account][container].lastModified = lastModified
    q()

  getContainer: (account, container) =>
    containerInfo = @containersStore[account][container]
    q(containerInfo)

  getContainers: (account) =>
    containerInfos = @containersStore[account]
    q(containerInfos)

  setContainerMetadata: (account, container, metadata) =>
    @getContainer(account, container).then (containerInfo) =>
      containerInfo.metadata = metadata

  setContainerAcl: (account, container, acl) =>
    @getContainer(account, container).then (containerInfo) =>
      containerInfo.acl = acl

  containerAddObject: (account, container, size) =>
    @containersStore[account][container].objectCount++
    @containersStore[account][container].bytesUsed += size
    q()

  containerDeleteObject: (account, container, size) =>
    @containersStore[account][container].objectCount--
    @containersStore[account][container].bytesUsed -= size
    q()

  # objects

  addObject: (account, container, object, obj) =>
    @objectsStore[account][container][object] = obj
    q()

  setObjectLastModified: (account, container, object, lastModified) =>
    @objectsStore[account][container][object].lastModified = lastModified
    q()

  deleteObject: (account, container, object) =>
    obj = @objectsStore[account][container][object]
    delete @objectsStore[account][container][object]
    q(obj)

  getObject: (account, container, object) =>
    object = @objectsStore[account][container]?[object]
    q(object)

  getObjects: (account, container) =>
    objects = @objectsStore[account][container]
    q(objects)

  setObjectMetadata: (account, container, object, metadata) =>
    @getObject(account, container, object).then (object) =>
      object.metadata = metadata

module.exports = MemoryBackend
