q = require('q')
_ = require('lodash')
should = require('chai').should()
require('mocha-as-promised')()

mongodb = require('mongodb')

MemoryBackend = require('../src/backend/memory')
MongoBackend = require('../src/backend/mongo')

[
  ['MemoryBackend', (-> q([new MemoryBackend(), (-> q())]))]
  ['MongoBackend', (->
    url = 'mongodb://localhost/lightswifttest'

    cleanup = ->
      q.ninvoke(mongodb, 'connect', url).then (db) ->
        q.ninvoke(db, 'collections').then (collections) ->
          collections = collections
            .map((x) -> x.collectionName)
            .filter((x) -> x.indexOf('system.') != 0)

          allDropped = collections.map (collection) ->
            q.ninvoke(db, 'dropCollection', collection)

          q.all(allDropped).then ->
            q.ninvoke(db, 'close')

    cleanup().then ->
      backend = new MongoBackend(url)

      backend.connect().then ->
        [backend, (-> backend.close().then(cleanup))]
  )]
].forEach ([name, getBackend]) ->
  describe name, ->
    backend = null
    cleanup = null

    setup = ->
      beforeEach (done) ->
        getBackend().then (b) ->
          [backend, cleanup] = b
          done()

      afterEach (done) ->
        backend = null
        cleanup().then(-> done())

    account = 'test'
    username = 'tester'
    key = 'testing'
    container = 'test-container'
    authToken = 'AUTH_tk08b15ec1692f5a3200f506fec5ea616f'
    object = 'dir1/file1'

    accountInfo =
      bytesUsed: 0
      containerCount: 0
      objectCount: 0
      metadata: {}
      lastModified: new Date()

    containerInfo =
      bytesUsed: 0
      objectCount: 0
      metadata: {}
      lastModified: new Date()
      acl:
        read: null
        write: null

    objectInfo =
      object: 'OBJID'
      metadata: {}
      contentType: 'text/plain'
      contentLength: 1024
      lastModified: new Date(Date.parse('Mon May 27 2013 16:48:48 GMT+0200 (CEST)'))
      hash: '08b15ec1692f5a3200f506fec5ea616f'

    describe 'account', ->
      setup()

      it 'should addAccount', ->
        backend.addAccount(account, _.cloneDeep(accountInfo)).then ->
          backend.getAccount(account).then (a) ->
            a.should.eql accountInfo

      it 'should setAccountLastModified', ->
        date = new Date(Date.parse('Mon May 27 2013 16:48:48 GMT+0200 (CEST)'))

        backend.addAccount(account, _.cloneDeep(accountInfo)).then ->
          backend.setAccountLastModified(account, date).then ->
            backend.getAccount(account).then (a) ->
              a.lastModified.should.eql date

      it 'should getAccount', ->
        backend.addAccount(account, _.cloneDeep(accountInfo)).then ->
          backend.getAccount(account).then (a) ->
            a.should.eql accountInfo

      it 'should setAccountMetadata', ->
        metadata = foo: 'bar'

        backend.addAccount(account, _.cloneDeep(accountInfo)).then ->
          backend.setAccountMetadata(account, _.cloneDeep(metadata)).then ->
            backend.getAccount(account).then (a) ->
              a.metadata.should.eql metadata

      it 'should accountAddContainer', ->
        backend.addAccount(account, _.cloneDeep(accountInfo)).then ->
          backend.accountAddContainer(account).then ->
            backend.getAccount(account).then (a) ->
              a.containerCount.should.equal 1

      it 'should accountDeleteContainer', ->
        backend.addAccount(account, _.cloneDeep(accountInfo)).then ->
          backend.accountAddContainer(account).then ->
            backend.getAccount(account).then (a) ->
              a.containerCount.should.equal 1

              backend.accountDeleteContainer(account).then ->
                backend.getAccount(account).then (a) ->
                  a.containerCount.should.equal 0

      it 'should accountAddObject', ->
        backend.addAccount(account, _.cloneDeep(accountInfo)).then ->
          backend.accountAddObject(account, 1024).then ->
            backend.getAccount(account).then (a) ->
              a.objectCount.should.equal 1
              a.bytesUsed.should.equal 1024

      it 'should accountDeleteObject', ->
        backend.addAccount(account, _.cloneDeep(accountInfo)).then ->
          backend.accountAddObject(account, 1024).then ->
            backend.getAccount(account).then (a) ->
              a.objectCount.should.equal 1
              a.bytesUsed.should.equal 1024

              backend.accountDeleteObject(account, 1024).then ->
                backend.getAccount(account).then (a) ->
                  a.objectCount.should.equal 0
                  a.bytesUsed.should.equal 0

    describe 'auth', ->
      setup()

      beforeEach (done) ->
        backend.addAccount(account, _.cloneDeep(accountInfo)).then(-> done())

      it 'should addUser', ->
        backend.addUser(account, username, key).then ->
          backend.getUser(account, username).then (u) ->
            u.should.have.ownProperty 'key'

      it 'should getUser', ->
        backend.addUser(account, username, key).then ->
          backend.getUser(account, username).then (u) ->
            u.should.have.ownProperty 'key'

      it 'should addAuthToken', ->
        backend.addAuthToken(account, authToken).then ->
          backend.getAuthTokenAccount(authToken).then (a) ->
            a.should.equal account

      it 'should getAuthTokenAccount', ->
        backend.addAuthToken(account, authToken).then ->
          backend.getAuthTokenAccount(authToken).then (a) ->
            a.should.equal account

    describe 'containers', ->
      setup()

      beforeEach (done) ->
        backend.addAccount(account, _.cloneDeep(accountInfo)).then(-> done())

      it 'should addContainer', ->
        backend.addContainer(account, container, _.cloneDeep(containerInfo)).then ->
          backend.getContainer(account, container).then (c) ->
            c.should.eql containerInfo

      it 'should deleteContainer', ->
        backend.addContainer(account, container, _.cloneDeep(containerInfo)).then ->
          backend.deleteContainer(account, container).then ->
            backend.getContainer(account, container).then (c) ->
              should.not.exist c

      it 'should setContainerLastModified', ->
        date = new Date(Date.parse('Mon May 27 2013 16:48:48 GMT+0200 (CEST)'))

        backend.addContainer(account, container, _.cloneDeep(containerInfo)).then ->
          backend.setContainerLastModified(account, container, date).then ->
            backend.getContainer(account, container).then (c) ->
              c.lastModified.should.eql date

      it 'should getContainer', ->
        backend.addContainer(account, container, _.cloneDeep(containerInfo)).then ->
          backend.getContainer(account, container).then (c) ->
            c.should.eql containerInfo

      it 'should getContainers', ->
        backend.addContainer(account, container, _.cloneDeep(containerInfo)).then ->
          backend.getContainers(account).then (cs) ->
            cs.should.eql _.object([[container, containerInfo]])

      it 'should setContainerMetadata', ->
        metadata = foo: 'bar'

        backend.addContainer(account, container, _.cloneDeep(containerInfo)).then ->
          backend.setContainerMetadata(account, container, _.cloneDeep(metadata)).then ->
            backend.getContainer(account, container).then (c) ->
              c.metadata.should.eql metadata

      it 'should setContainerAcl', ->
        acl =
          read: '.r:*'
          write: 'foo'

        backend.addContainer(account, container, _.cloneDeep(containerInfo)).then ->
          backend.setContainerAcl(account, container, _.cloneDeep(acl)).then ->
            backend.getContainer(account, container).then (c) ->
              c.acl.should.eql acl

      it 'should containerAddObject', ->
        backend.addContainer(account, container, _.cloneDeep(containerInfo)).then ->
          backend.containerAddObject(account, container, 1024).then ->
            backend.getContainer(account, container).then (c) ->
              c.objectCount.should.equal 1
              c.bytesUsed.should.equal 1024

      it 'should containerDeleteObject', ->
        backend.addContainer(account, container, _.cloneDeep(containerInfo)).then ->
          backend.containerAddObject(account, container, 1024).then ->
            backend.getContainer(account, container).then (c) ->
              c.objectCount.should.equal 1
              c.bytesUsed.should.equal 1024

              backend.containerDeleteObject(account, container, 1024).then ->
                backend.getContainer(account, container).then (c) ->
                  c.objectCount.should.equal 0
                  c.bytesUsed.should.equal 0

    describe 'objects', ->
      setup()

      beforeEach (done) ->
        backend.addAccount(account, _.cloneDeep(accountInfo))
          .then(-> backend.addContainer(account, container, _.cloneDeep(containerInfo)))
          .then(-> done())

      it 'should addObject', ->
        backend.addObject(account, container, object, _.cloneDeep(objectInfo)).then ->
          backend.getObject(account, container, object).then (o) ->
           o.should.eql objectInfo

      it 'should setObjectLastModified', ->
        date = new Date(Date.parse('Mon May 27 2013 16:48:48 GMT+0200 (CEST)'))

        backend.addObject(account, container, object, _.cloneDeep(objectInfo)).then ->
          backend.setObjectLastModified(account, container, object, date).then ->
            backend.getObject(account, container, object).then (o) ->
             o.lastModified.should.eql date

      it 'should deleteObject', ->
        backend.addObject(account, container, object, _.cloneDeep(objectInfo)).then ->
          backend.deleteObject(account, container, object).then ->
            backend.getObject(account, container, object).then (o) ->
              should.not.exist o

      it 'should getObject', ->
        backend.addObject(account, container, object, _.cloneDeep(objectInfo)).then ->
          backend.getObject(account, container, object).then (o) ->
           o.should.eql objectInfo

      it 'should getObjects', ->
        backend.addObject(account, container, object, _.cloneDeep(objectInfo)).then ->
          backend.getObjects(account, container).then (os) ->
            os.should.eql _.object([[object, objectInfo]])

      it 'should setObjectMetadata', ->
        metadata = foo: 'bar'

        backend.addObject(account, container, object, _.cloneDeep(objectInfo)).then ->
          backend.setObjectMetadata(account, container, object, _.cloneDeep(metadata)).then ->
            backend.getObject(account, container, object).then (o) ->
             o.metadata.should.eql metadata
