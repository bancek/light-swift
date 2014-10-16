q = require('q')
_ = require('lodash')
mysql = require('mysql')

class MysqlBackend
  constructor: (url) ->
    @databaseUrl = url or 'mysql://lightswift:lightswift@localhost/lightswift?charset=utf8'

  connect: =>
    @pool = mysql.createPool(@databaseUrl)

    q.all [
      @createAccountTable()
      @createAuthTable()
      @createAuthTokensTable()
      @createContainerTable()
      @createObjectTable()
    ]

  createAccountTable: =>
    q.ninvoke @pool, 'query', '''
    CREATE TABLE IF NOT EXISTS `account` (
      `account` varchar(255) NOT NULL,
      `bytes_used` BIGINT unsigned NOT NULL,
      `container_count` BIGINT unsigned NOT NULL,
      `object_count` BIGINT unsigned NOT NULL,
      `last_modified` BIGINT unsigned NOT NULL,
      `metadata` BLOB NOT NULL,
      PRIMARY KEY (`account`)
    ) ENGINE=InnoDB DEFAULT CHARSET utf8 COLLATE utf8_general_ci;
    '''

  createAuthTable: =>
    q.ninvoke @pool, 'query', '''
    CREATE TABLE IF NOT EXISTS `auth` (
      `account` varchar(255) NOT NULL,
      `username` varchar(255) NOT NULL,
      `key` varchar(255) NOT NULL,
      PRIMARY KEY (`account`, `username`)
    ) ENGINE=InnoDB DEFAULT CHARSET utf8 COLLATE utf8_general_ci;
    '''

  createAuthTokensTable: =>
    q.ninvoke @pool, 'query', '''
    CREATE TABLE IF NOT EXISTS `authtoken` (
      `token` varchar(255) NOT NULL,
      `account` varchar(255) NOT NULL,
      PRIMARY KEY (`token`)
    ) ENGINE=InnoDB DEFAULT CHARSET utf8 COLLATE utf8_general_ci;
    '''

  createContainerTable: =>
    q.ninvoke @pool, 'query', '''
    CREATE TABLE IF NOT EXISTS `container` (
      `account` varchar(255) NOT NULL,
      `container` varchar(255) NOT NULL,
      `bytes_used` BIGINT unsigned NOT NULL,
      `object_count` BIGINT unsigned NOT NULL,
      `last_modified` BIGINT unsigned NOT NULL,
      `metadata` BLOB NOT NULL,
      `acl_read` varchar(255),
      `acl_write` varchar(255),
      PRIMARY KEY (`account`, `container`)
    ) ENGINE=InnoDB DEFAULT CHARSET utf8 COLLATE utf8_general_ci;
    '''

  createObjectTable: =>
    q.ninvoke @pool, 'query', '''
    CREATE TABLE IF NOT EXISTS `object` (
      `account` varchar(255) NOT NULL,
      `container` varchar(255) NOT NULL,
      `object` varchar(255) NOT NULL,
      `backend_object` varchar(255) NOT NULL,
      `object_manifest` varchar(255),
      `content_type` varchar(255) NOT NULL,
      `hash` varchar(255) NOT NULL,
      `content_length` BIGINT unsigned NOT NULL,
      `last_modified` BIGINT unsigned NOT NULL,
      `metadata` BLOB NOT NULL,
      PRIMARY KEY (`account`, `container`, `object`)
    ) ENGINE=InnoDB DEFAULT CHARSET utf8 COLLATE utf8_general_ci;
    '''

  close: =>
    return q() if not @pool
    q.ninvoke(@pool, 'end')

  # accounts

  addAccount: (account, accountInfo) =>
    q.ninvoke @pool, 'query', '''
      INSERT INTO `account` (
        `account`,
        `bytes_used`,
        `container_count`,
        `object_count`,
        `last_modified`,
        `metadata`
      ) VALUES (?, ?, ?, ?, ?, ?)
    ''', [
      account
      accountInfo.bytesUsed
      accountInfo.containerCount
      accountInfo.objectCount
      accountInfo.lastModified.getTime()
      JSON.stringify(accountInfo.metadata)
    ]

  setAccountLastModified: (account, lastModified) =>
    q.ninvoke @pool, 'query', '''
      UPDATE `account`
      SET
        `last_modified` = ?
      WHERE
        `account` = ?
    ''', [
      lastModified.getTime()
      account
    ]

  getAccount: (account) =>
    q.ninvoke(@pool, 'query', '''
      SELECT
        `bytes_used`,
        `container_count`,
        `object_count`,
        `last_modified`,
        `metadata`
      FROM `account`
      WHERE
        `account` = ?
    ''', [
      account
    ]).then (rows) ->
      res = rows[0][0]
      return if not res

      bytesUsed: res.bytes_used
      containerCount: res.container_count
      objectCount: res.object_count
      lastModified: new Date(res.last_modified)
      metadata: JSON.parse(res.metadata)

  setAccountMetadata: (account, metadata) =>
    q.ninvoke @pool, 'query', '''
      UPDATE `account`
      SET
        `metadata` = ?
      WHERE
        `account` = ?
    ''', [
      JSON.stringify(metadata)
      account
    ]

  accountAddContainer: (account) =>
    q.ninvoke @pool, 'query', '''
      UPDATE `account`
      SET
        `container_count` = `container_count` + 1
      WHERE
        `account` = ?
    ''', [
      account
    ]

  accountDeleteContainer: (account) =>
    q.ninvoke @pool, 'query', '''
      UPDATE `account`
      SET
        `container_count` = `container_count` - 1
      WHERE
        `account` = ?
    ''', [
      account
    ]

  accountAddObject: (account, size) =>
    q.ninvoke @pool, 'query', '''
      UPDATE `account`
      SET
        `bytes_used` = `bytes_used` + ?,
        `object_count` = `object_count` + 1
      WHERE
        `account` = ?
    ''', [
      size,
      account
    ]

  accountDeleteObject: (account, size) =>
    q.ninvoke @pool, 'query', '''
      UPDATE `account`
      SET
        `bytes_used` = `bytes_used` - ?,
        `object_count` = `object_count` - 1
      WHERE
        `account` = ?
    ''', [
      size,
      account
    ]

  # auth

  addUser: (account, username, key) =>
    q.ninvoke @pool, 'query', '''
      INSERT INTO `auth` (
        `account`,
        `username`,
        `key`
      ) VALUES (?, ?, ?)
    ''', [
      account
      username
      key
    ]

  getUser: (account, username) =>
    q.ninvoke(@pool, 'query', '''
      SELECT
        `key`
      FROM `auth`
      WHERE
        `account` = ? AND
        `username` = ?
    ''', [
      account
      username
    ]).then (rows) ->
      res = rows[0][0]
      return if not res

      key: res.key

  addAuthToken: (account, authToken) =>
    q.ninvoke @pool, 'query', '''
      INSERT INTO `authtoken` (
        `token`,
        `account`
      ) VALUES (?, ?)
    ''', [
      authToken
      account
    ]

  getAuthTokenAccount: (authToken) =>
    q.ninvoke(@pool, 'query', '''
      SELECT
        `account`
      FROM `authtoken`
      WHERE
        `token` = ?
    ''', [
      authToken
    ]).then (rows) ->
      res = rows[0][0]
      return if not res

      res.account

  # containers

  addContainer: (account, container, containerInfo) =>
    q.ninvoke @pool, 'query', '''
      INSERT INTO `container` (
        `account`,
        `container`,
        `bytes_used`,
        `object_count`,
        `last_modified`,
        `metadata`,
        `acl_read`,
        `acl_write`
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', [
      account
      container
      containerInfo.bytesUsed
      containerInfo.objectCount
      containerInfo.lastModified.getTime()
      JSON.stringify(containerInfo.metadata)
      null
      null
    ]

  deleteContainer: (account, container) =>
    q.ninvoke @pool, 'query', '''
      DELETE FROM `container`
      WHERE
        `account` = ? AND
        `container` = ?
    ''', [
      account
      container
    ]

  setContainerLastModified: (account, container, lastModified) =>
    q.ninvoke @pool, 'query', '''
      UPDATE `container`
      SET
        `last_modified` = ?
      WHERE
        `account` = ? AND
        `container` = ?
    ''', [
      lastModified.getTime()
      account
      container
    ]

  _rowToContainer: (row) =>
    bytesUsed: row.bytes_used
    objectCount: row.object_count
    lastModified: new Date(row.last_modified)
    metadata: JSON.parse(row.metadata)
    acl:
      read: row.acl_read
      write: row.acl_write

  getContainer: (account, container) =>
    q.ninvoke(@pool, 'query', '''
      SELECT
        `bytes_used`,
        `object_count`,
        `last_modified`,
        `metadata`,
        `acl_read`,
        `acl_write`
      FROM `container`
      WHERE
        `account` = ? AND
        `container` = ?
    ''', [
      account
      container
    ]).then (rows) =>
      res = rows[0][0]
      return if not res

      @_rowToContainer(res)

  getContainers: (account) =>
    q.ninvoke(@pool, 'query', '''
      SELECT
        `container`,
        `bytes_used`,
        `object_count`,
        `last_modified`,
        `metadata`,
        `acl_read`,
        `acl_write`
      FROM `container`
      WHERE
        `account` = ?
    ''', [
      account
    ]).then (rows) =>
      res = rows[0]

      _(res)
        .map((x) => [x.container, @_rowToContainer(x)])
        .object()
        .value()

  setContainerMetadata: (account, container, metadata) =>
    q.ninvoke @pool, 'query', '''
      UPDATE `container`
      SET
        `metadata` = ?
      WHERE
        `account` = ? AND
        `container` = ?
    ''', [
      JSON.stringify(metadata)
      account
      container
    ]

  setContainerAcl: (account, container, acl) =>
    q.ninvoke @pool, 'query', '''
      UPDATE `container`
      SET
        `acl_read` = ?,
        `acl_write` = ?
      WHERE
        `account` = ? AND
        `container` = ?
    ''', [
      acl.read,
      acl.write
      account
      container
    ]

  containerAddObject: (account, container, size) =>
    q.ninvoke @pool, 'query', '''
      UPDATE `container`
      SET
        `bytes_used` = `bytes_used` + ?,
        `object_count` = `object_count` + 1
      WHERE
        `account` = ? AND
        `container` = ?
    ''', [
      size,
      account
      container
    ]

  containerDeleteObject: (account, container, size) =>
    q.ninvoke @pool, 'query', '''
      UPDATE `container`
      SET
        `bytes_used` = `bytes_used` - ?,
        `object_count` = `object_count` - 1
      WHERE
        `account` = ? AND
        `container` = ?
    ''', [
      size,
      account
      container
    ]

  # objects

  addObject: (account, container, object, obj) =>
    q.ninvoke @pool, 'query', '''
      INSERT INTO `object` (
        `account`,
        `container`,
        `object`,
        `backend_object`,
        `object_manifest`,
        `content_type`,
        `hash`,
        `content_length`,
        `last_modified`,
        `metadata`
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', [
      account
      container
      object
      obj.object
      obj.objectManifest
      obj.contentType
      obj.hash
      obj.contentLength
      obj.lastModified.getTime()
      JSON.stringify(obj.metadata)
    ]

  setObjectLastModified: (account, container, object, lastModified) =>
    q.ninvoke @pool, 'query', '''
      UPDATE `object`
      SET
        `last_modified` = ?
      WHERE
        `account` = ? AND
        `container` = ? AND
        `object` = ?
    ''', [
      lastModified.getTime()
      account
      container
      object
    ]

  deleteObject: (account, container, object) =>
    q.ninvoke @pool, 'query', '''
      DELETE FROM `object`
      WHERE
        `account` = ? AND
        `container` = ? AND
        `object` = ?
    ''', [
      account
      container
      object
    ]

  _rowToObject: (row) =>
    object: row.backend_object
    objectManifest: row.object_manifest
    contentType: row.content_type
    hash: row.hash
    contentLength: row.content_length
    lastModified: new Date(row.last_modified)
    metadata: JSON.parse(row.metadata)

  getObject: (account, container, object) =>
    q.ninvoke(@pool, 'query', '''
      SELECT
        `backend_object`,
        `object_manifest`,
        `content_type`,
        `hash`,
        `content_length`,
        `last_modified`,
        `metadata`
      FROM `object`
      WHERE
        `account` = ? AND
        `container` = ? AND
        `object` = ?
    ''', [
      account
      container
      object
    ]).then (rows) =>
      res = rows[0][0]
      return if not res

      @_rowToObject(res)

  getObjects: (account, container) =>
    q.ninvoke(@pool, 'query', '''
      SELECT
        `object`,
        `backend_object`,
        `object_manifest`,
        `content_type`,
        `hash`,
        `content_length`,
        `last_modified`,
        `metadata`
      FROM `object`
      WHERE
        `account` = ? AND
        `container` = ?
    ''', [
      account
      container
    ]).then (rows) =>
      res = rows[0]

      _(res)
        .map((x) => [x.object, @_rowToObject(x)])
        .object()
        .value()

  setObjectMetadata: (account, container, object, metadata) =>
    q.ninvoke @pool, 'query', '''
      UPDATE `object`
      SET
        `metadata` = ?
      WHERE
        `account` = ? AND
        `container` = ? AND
        `object` = ?
    ''', [
      JSON.stringify(metadata)
      account
      container
      object
    ]

module.exports = MysqlBackend
