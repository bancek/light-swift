q = require('q')
_ = require('lodash')
request = require('request')
chai = require('chai')
should = require('chai').should()
require('mocha-as-promised')()

LightSwift = require('../src/light-swift')

authUrl = null
authToken = null
storageUrl = null

makeRequest = (method, uri, options) ->
  baseUrl = if storageUrl? then storageUrl else authUrl

  opts =
    method: method
    uri: baseUrl + uri

  _.merge(opts, options)

  if authToken? and not options?.noAuth
    opts.headers = _.merge({}, opts.headers, {'x-auth-token': authToken})

  q.nfcall(request, opts)

GET = _.partial(makeRequest, 'GET')
HEAD = _.partial(makeRequest, 'HEAD')
POST = _.partial(makeRequest, 'POST')
PUT = _.partial(makeRequest, 'PUT')
DELETE = _.partial(makeRequest, 'DELETE')
COPY = _.partial(makeRequest, 'COPY')

describe 'Swift API', ->
  swiftUser = null
  swiftKey = null

  setup = ->
    if process.env.ST_AUTH
      beforeEach (done) ->
        authUrl = process.env.ST_AUTH
        swiftUser = process.env.ST_USER or 'test:tester'
        swiftKey = process.env.ST_KEY or 'testing'

        doAuth().then(->
          GET("/", json: yes).then ([res, body]) ->
            q.all(body.map (c) ->
              GET("/#{c.name}", json: yes).then ([res, body]) ->
                q.all(body.map (o) ->
                  DELETE("/#{c.name}/#{o.name}")
                ).then(->
                  DELETE("/#{c.name}")
                )
            )
        ).then(->
          clearAuth()
          done()
        )

    else
      swift = null

      beforeEach (done) ->
        authUrl = 'http://localhost:8080/auth/v1.0'
        swiftUser = 'test:tester'
        swiftKey = 'testing'

        swift = new LightSwift(port: 8080)

        swift.connect()
          .then(-> swift.addAccount('test'))
          .then(-> swift.addUser('test', 'tester', 'testing'))
          .then(-> swift.server())
          .then(-> done())

      afterEach (done) ->
        authUrl = null
        swift.close().then(-> done())

  doAuth = ->
    headers =
      'X-Auth-User': swiftUser
      'X-Auth-Key': swiftKey

    GET('', headers: headers).then ([res, body]) ->
      res.statusCode.should.equal 200
      authToken = res.headers['x-auth-token']
      storageUrl = res.headers['x-storage-url']

  clearAuth = ->
    authToken = null
    storageUrl = null

  auth = ->
    beforeEach (done) ->
      doAuth()

    afterEach ->
      clearAuth()

  withContainer = ->
    beforeEach (done) ->
      PUT('/test-container').then(-> done())

  createObjects = (objects) ->
    created = objects.map (obj) ->
      if _.isString(obj)
        obj = [obj]

      contentType = obj[2] or 'text/plain'

      if obj[0].slice(-1) == '/'
        contentType = 'application/directory'
        obj[0] = obj[0].slice(0, -1)

      opts =
        body: obj[1] or '123'
        headers:
          'content-type': contentType

      PUT("/test-container/#{obj[0]}", opts)

    q.all(created)

  createSampleObjects = ->
    objects = [
      'file'
      'dir1/file1.txt'
      'dir1/file2.txt'
      'dir1/dir2/file3.txt'
      'dir1/dir3/'
      'dir1/dir4/'
      'dirX/'
      'dirX/f1'
      'another'
    ]

    createObjects(objects)

  withObject = ->
    beforeEach (done) ->
      createObjects([['file1', '123']]).then(-> done())

  describe 'auth', ->
    setup()

    it 'should authenticate', (done) ->
      GET('', headers: {'X-Auth-User': swiftUser, 'X-Auth-Key': swiftKey})
        .then ([res, body]) ->
          res.statusCode.should.equal 200
          res.headers.should.have.property 'x-storage-url'
          res.headers.should.have.property 'x-auth-token'
          res.headers.should.have.property 'x-storage-token'
          res.headers['x-storage-token'].should.equal res.headers['x-auth-token']

    it 'should fail to authenticate', (done) ->
      GET('', headers: {'X-Auth-User': 'non:existing', 'X-Auth-Key': 'user'})
        .then ([res, body]) ->
          res.statusCode.should.equal 401

  describe 'unauthorized', ->
    setup()
    auth()

    it 'should not get account info without auth token', (done) ->
      authToken = ''

      HEAD('/').then ([res, body]) ->
        res.statusCode.should.equal 401

    it 'should not get account info with invalid auth token', (done) ->
      authToken = 'TOKEN'

      HEAD('/').then ([res, body]) ->
        res.statusCode.should.equal 401

    it 'should not get account info with non-existing account', (done) ->
      storageUrl = storageUrl + '_NONEXISTING'

      HEAD('/').then ([res, body]) ->
        res.statusCode.should.equal 403

  describe 'json', ->
    setup()
    auth()

    checkJson = ([res, body]) ->
      res.headers['content-type'].toLowerCase().should.equal 'application/json; charset=utf-8'

    it 'should be JSON format=json', (done) ->
      GET('/?format=json').then(checkJson)

    it 'should be JSON accept application/json', (done) ->
      GET('/', headers: {accept: 'application/json'}).then(checkJson)

    # it 'should be JSON accept text/json', (done) ->
    #   GET('/', headers: {accept: 'text/json'}).then(checkJson)

    # it 'should be JSON content-type application/json', (done) ->
    #   GET('/', headers: {'content-type': 'application/json'}).then(checkJson)

    # it 'should be JSON content-type application/json charset=utf-8', (done) ->
    #   GET('/', headers: {'content-type': 'application/json; charset=utf-8'}).then(checkJson)

    # it 'should be JSON content-type text/json', (done) ->
    #   GET('/', headers: {'content-type': 'text/json'}).then(checkJson)

    # it 'should be JSON content-type text/json charset=utf-8', (done) ->
    #   GET('/', headers: {'content-type': 'text/json; charset=utf-8'}).then(checkJson)

  describe 'account', ->
    setup()
    auth()

    it 'should get account info', (done) ->
      HEAD('/').then ([res, body]) ->
        res.statusCode.should.equal 204
        res.headers.should.have.property 'x-trans-id'
        res.headers['x-account-bytes-used'].should.equal '0'
        res.headers['x-account-container-count'].should.equal '0'
        res.headers['x-account-object-count'].should.equal '0'
        res.headers.should.have.property 'x-timestamp'

    it 'should add and delete account metadata', (done) ->
      headers =
        'x-account-meta-foo': '123'
        'x-account-meta-bar': '456'

      POST('/', headers: headers).then ([res, body]) ->
        res.statusCode.should.equal 204

        HEAD('/').then ([res, body]) ->
          res.headers['x-account-meta-foo'].should.equal '123'
          res.headers['x-account-meta-bar'].should.equal '456'

          headers =
            'x-account-meta-bar': ''

          POST('/', headers: headers).then ([res, body]) ->
            HEAD('/').then ([res, body]) ->
              res.headers['x-account-meta-foo'].should.equal '123'
              res.headers.should.not.have.property 'x-account-meta-bar'

              headers =
                'x-remove-account-meta-foo': 'x'

              POST('/', headers: headers).then ([res, body]) ->
                HEAD('/').then ([res, body]) ->
                  res.headers.should.not.have.property 'x-account-meta-foo'

    it 'should fail to put account', (done) ->
      PUT('/').then ([res, body]) ->
        res.statusCode.should.equal 403

    it 'should fail to delete account', (done) ->
      DELETE('/').then ([res, body]) ->
        res.statusCode.should.equal 403

    it 'should get empty containers', (done) ->
      GET('/').then ([res, body]) ->
        res.statusCode.should.equal 204
        res.body.should.equal ''

    it 'should get containers', (done) ->
      PUT('/container1').then ->
        PUT('/container2').then ->
          GET('/').then ([res, body]) ->
            res.statusCode.should.equal 200
            res.headers['x-account-container-count'].should.equal '2'
            body.split('\n').should.have.els ['container1', 'container2', '']

    it 'should get containers with marker', (done) ->
      PUT('/container1').then ->
        PUT('/container2').then ->
          GET('/?marker=container2').then ([res, body]) ->
            res.statusCode.should.equal 204
            res.headers['x-account-container-count'].should.equal '2'

    it 'should get empty containers (JSON)', (done) ->
      GET('/', json: yes).then ([res, body]) ->
        res.statusCode.should.equal 200
        body.should.eql []

    it 'should get containers (JSON)', (done) ->
      PUT('/container1').then ->
        GET('/', json: yes).then ([res, body]) ->
          body.should.eql [
            name: 'container1'
            count: 0
            bytes: 0
          ]

  describe 'container', ->
    setup()
    auth()
    withContainer()

    it 'should get container info', (done) ->
      HEAD('/test-container').then ([res, body]) ->
        res.statusCode.should.equal 204
        res.headers.should.have.property 'x-trans-id'
        res.headers['x-container-bytes-used'].should.equal '0'
        res.headers['x-container-object-count'].should.equal '0'
        res.headers.should.have.property 'x-timestamp'

    it 'should add and delete container metadata', (done) ->
      headers =
        'x-container-meta-foo': '123'
        'x-container-meta-bar': '456'

      POST('/test-container', headers: headers).then ([res, body]) ->
        res.statusCode.should.equal 204

        HEAD('/test-container').then ([res, body]) ->
          res.headers['x-container-meta-foo'].should.equal '123'
          res.headers['x-container-meta-bar'].should.equal '456'

          headers =
            'x-container-meta-bar': ''

          POST('/test-container', headers: headers).then ->
            HEAD('/test-container').then ([res, body]) ->
              res.headers['x-container-meta-foo'].should.equal '123'
              res.headers.should.not.have.property 'x-container-meta-bar'

              headers =
                'x-remove-container-meta-foo': 'x'

              POST('/test-container', headers: headers).then ->
                HEAD('/test-container').then ([res, body]) ->
                  res.headers.should.not.have.property 'x-container-meta-foo'

    it 'should fail to add metadata to non-existing container', (done) ->
      headers =
        'x-container-meta-foo': '123'

      POST('/non-existing', headers: headers).then ([res, body]) ->
        res.statusCode.should.equal 404

    it 'should fail to put existing container', (done) ->
      PUT('/test-container').then ([res, body]) ->
        res.statusCode.should.equal 202

    it 'should fail to put existing container metadata', (done) ->
      headers =
        'x-container-meta-foo': '123'

      PUT('/test-container', headers: headers).then ([res, body]) ->
        res.statusCode.should.equal 202

        HEAD('/container1').then ([res, body]) ->
          res.headers.should.not.have.property 'x-container-meta-foo'

    it 'should put new container', (done) ->
      PUT('/container1').then ([res, body]) ->
        res.statusCode.should.equal 201

        HEAD('/container1').then ([res, body]) ->
          res.statusCode.should.equal 204

    it 'should put new container with metadata', (done) ->
      headers =
        'x-container-meta-foo': '123'

      PUT('/container1', headers: headers).then ([res, body]) ->
        res.statusCode.should.equal 201

        HEAD('/container1').then ([res, body]) ->
          res.headers['x-container-meta-foo'].should.equal '123'

    it 'should put new container with ACL', (done) ->
      headers =
        'x-container-read': '.r:*'
        'x-container-write': 'foo'

      PUT('/container1', headers: headers).then ([res, body]) ->
        res.statusCode.should.equal 201

        HEAD('/container1').then ([res, body]) ->
          res.headers['x-container-read'].should.equal '.r:*'
          res.headers['x-container-write'].should.equal 'foo'

    it 'should put existing container with ACL', (done) ->
      headers =
        'x-container-read': '.r:*'
        'x-container-write': 'foo'

      PUT('/test-container', headers: headers).then ([res, body]) ->
        res.statusCode.should.equal 202

        HEAD('/test-container').then ([res, body]) ->
          res.headers['x-container-read'].should.equal '.r:*'
          res.headers['x-container-write'].should.equal 'foo'

    it 'should post existing container with ACL', (done) ->
      headers =
        'x-container-read': '.r:*'
        'x-container-write': 'foo'

      POST('/test-container', headers: headers).then ([res, body]) ->
        res.statusCode.should.equal 204

        HEAD('/test-container').then ([res, body]) ->
          res.headers['x-container-read'].should.equal '.r:*'
          res.headers['x-container-write'].should.equal 'foo'

    it 'should delete empty container', (done) ->
      PUT('/empty').then ->
        DELETE('/empty').then ([res, body]) ->
          res.statusCode.should.equal 204

    it 'should not delete container with objects', (done) ->
      PUT('/test-container/file1', body: '123').then ->
        DELETE('/test-container').then ([res, body]) ->
          res.statusCode.should.equal 409

    it 'should not delete non-existing container', (done) ->
      DELETE('/non-existing').then ([res, body]) ->
        res.statusCode.should.equal 404

    it 'should get empty objects (plain)', (done) ->
      GET('/test-container').then ([res, body]) ->
        res.statusCode.should.equal 204
        res.body.should.equal ''

    it 'should get objects (plain)', (done) ->
      PUT('/test-container/file1', body: '123').then ->
        PUT('/test-container/file2', body: '456').then ->
          GET('/test-container').then ([res, body]) ->
            res.statusCode.should.equal 200
            res.headers['x-container-object-count'].should.equal '2'
            res.headers['x-container-bytes-used'].should.equal '6'
            body.split('\n').should.have.els ['file1', 'file2', '']

    it 'should get empty objects (JSON)', (done) ->
      GET('/test-container', json: yes).then ([res, body]) ->
        res.statusCode.should.equal 200
        body.should.eql []

    it 'should get objects (JSON)', (done) ->
      createObjects([['file1', '123']]).then ->
        GET('/test-container', json: yes).then ([res, body]) ->
          body.length.should.equal 1
          obj = body[0]
          obj.should.have.keys 'bytes', 'content_type', 'hash', 'last_modified', 'name'
          obj.name.should.equal 'file1'
          obj.bytes.should.equal 3
          obj.hash.should.equal '202cb962ac59075b964b07152d234b70'
          obj.content_type.should.equal 'text/plain'

    it 'should get objects', (done) ->
      createSampleObjects().then ->
        GET('/test-container', json: yes).then ([res, body]) ->
          body = _.map body, (x) -> x.name or x.subdir

          body.should.eqlSorted [
            'file'
            'dir1/file1.txt'
            'dir1/file2.txt'
            'dir1/dir2/file3.txt'
            'dir1/dir3'
            'dir1/dir4'
            'dirX'
            'dirX/f1'
            'another'
          ]

    it 'should get objects with position marker', (done) ->
      createSampleObjects().then ->
        GET('/test-container?marker=dirX', json: yes).then ([res, body]) ->
          body.length.should.equal 0

    it 'should get objects with empty prefix', (done) ->
      createSampleObjects().then ->
        GET('/test-container?prefix=', json: yes).then ([res, body]) ->
          body = _.map body, (x) -> x.name or x.subdir

          body.should.eqlSorted [
            'file'
            'dir1/file1.txt'
            'dir1/file2.txt'
            'dir1/dir2/file3.txt'
            'dir1/dir3'
            'dir1/dir4'
            'dirX'
            'dirX/f1'
            'another'
          ]

    it 'should get objects with prefix', (done) ->
      createSampleObjects().then ->
        GET('/test-container?prefix=dir1', json: yes).then ([res, body]) ->
          body = _.map body, (x) -> x.name or x.subdir

          body.should.eqlSorted [
            'dir1/file1.txt'
            'dir1/file2.txt'
            'dir1/dir2/file3.txt'
            'dir1/dir3'
            'dir1/dir4'
          ]

    it 'should get objects with prefix/', (done) ->
      createSampleObjects().then ->
        GET('/test-container?prefix=dir1/', json: yes).then ([res, body]) ->
          body = _.map body, (x) -> x.name or x.subdir

          body.should.eqlSorted [
            'dir1/file1.txt'
            'dir1/file2.txt'
            'dir1/dir2/file3.txt'
            'dir1/dir3'
            'dir1/dir4'
          ]

    it 'should get objects with prefix (marked)', (done) ->
      createSampleObjects().then ->
        GET('/test-container?prefix=dirX', json: yes).then ([res, body]) ->
          body = _.map body, (x) -> x.name or x.subdir

          body.should.eqlSorted [
            'dirX'
            'dirX/f1'
          ]

    it 'should get objects with prefix/ (marked)', (done) ->
      createSampleObjects().then ->
        GET('/test-container?prefix=dirX/', json: yes).then ([res, body]) ->
          body = _.map body, (x) -> x.name or x.subdir

          body.should.eqlSorted [
            'dirX/f1'
          ]

    it 'should get objects with empty prefix and delimiter', (done) ->
      createSampleObjects().then ->
        GET('/test-container?prefix=&delimiter=/', json: yes).then ([res, body]) ->
          body = _.map body, (x) -> if x.name then name: x.name else subdir: x.subdir
          body = _.sortBy body, (x) -> x.name or x.subdir

          body.should.eql [
            { name: 'another' }
            { subdir: 'dir1/' }
            { name: 'dirX' }
            { subdir: 'dirX/' }
            { name: 'file' }
          ]

    it 'should get objects with prefix and delimiter', (done) ->
      createSampleObjects().then ->
        GET('/test-container?prefix=dir1&delimiter=/', json: yes).then ([res, body]) ->
          body = _.map body, (x) -> if x.name then name: x.name else subdir: x.subdir
          body = _.sortBy body, (x) -> x.name or x.subdir

          body.should.eql [
            { subdir: 'dir1/' }
          ]

    it 'should get objects with prefix/ and delimiter', (done) ->
      createSampleObjects().then ->
        GET('/test-container?prefix=dir1/&delimiter=/', json: yes).then ([res, body]) ->
          body = _.map body, (x) -> if x.name then name: x.name else subdir: x.subdir
          body = _.sortBy body, (x) -> x.name or x.subdir

          body.should.eql [
            { subdir: 'dir1/dir2/' }
            { name: 'dir1/dir3' }
            { name: 'dir1/dir4' }
            { name: 'dir1/file1.txt' }
            { name: 'dir1/file2.txt' }
          ]

    it 'should get objects with empty path', (done) ->
      createSampleObjects().then ->
        GET('/test-container?path=', json: yes).then ([res, body]) ->
          body = _.map body, (x) -> if x.name then name: x.name else subdir: x.subdir
          body = _.sortBy body, (x) -> x.name or x.subdir

          body.should.eql [
            { name: 'another' },
            { name: 'dirX' },
            { name: 'file' },
          ]

    it 'should get objects with path', (done) ->
      createSampleObjects().then ->
        GET('/test-container?path=dir1', json: yes).then ([res, body]) ->
          body = _.map body, (x) -> if x.name then name: x.name else subdir: x.subdir
          body = _.sortBy body, (x) -> x.name or x.subdir

          body.should.eql [
            { name: 'dir1/dir3' }
            { name: 'dir1/dir4' }
            { name: 'dir1/file1.txt' }
            { name: 'dir1/file2.txt' }
          ]

    it 'should get objects with path/', (done) ->
      createSampleObjects().then ->
        GET('/test-container?path=dir1/', json: yes).then ([res, body]) ->
          body = _.map body, (x) -> if x.name then name: x.name else subdir: x.subdir
          body = _.sortBy body, (x) -> x.name or x.subdir

          body.should.eql [
            { name: 'dir1/dir3' }
            { name: 'dir1/dir4' }
            { name: 'dir1/file1.txt' }
            { name: 'dir1/file2.txt' }
          ]

    it 'should get objects with path inner/', (done) ->
      createSampleObjects().then ->
        GET('/test-container?path=dir1/dir2/', json: yes).then ([res, body]) ->
          body = _.map body, (x) -> if x.name then name: x.name else subdir: x.subdir
          body = _.sortBy body, (x) -> x.name or x.subdir

          body.should.eql [
            { name: 'dir1/dir2/file3.txt' }
          ]

    it 'should get objects with path (marked)', (done) ->
      createSampleObjects().then ->
        GET('/test-container?path=dirX', json: yes).then ([res, body]) ->
          body = _.map body, (x) -> if x.name then name: x.name else subdir: x.subdir
          body = _.sortBy body, (x) -> x.name or x.subdir

          body.should.eql [
            { name: 'dirX/f1' }
          ]

    it 'should get objects with path/ (marked)', (done) ->
      createSampleObjects().then ->
        GET('/test-container?path=dirX/', json: yes).then ([res, body]) ->
          body = _.map body, (x) -> if x.name then name: x.name else subdir: x.subdir
          body = _.sortBy body, (x) -> x.name or x.subdir

          body.should.eql [
            { name: 'dirX/f1' }
          ]

    it 'should get objects with nonexisting path', (done) ->
      createSampleObjects().then ->
        GET('/test-container?path=nonexisting', json: yes).then ([res, body]) ->
          body.length.should.equal 0

  describe 'object', ->
    setup()
    auth()
    withContainer()
    withObject()

    it 'should get object info', (done) ->
      HEAD('/test-container/file1').then ([res, body]) ->
        res.statusCode.should.equal 200
        res.headers['content-type'].should.equal 'text/plain'
        res.headers['content-length'].should.equal '3'
        res.headers['etag'].should.equal '202cb962ac59075b964b07152d234b70'
        res.headers.should.have.property 'last-modified'
        res.headers.should.have.property 'x-trans-id'
        res.headers.should.have.property 'x-timestamp'

    it 'should get object', (done) ->
      GET('/test-container/file1').then ([res, body]) ->
        res.statusCode.should.equal 200
        res.headers['content-type'].should.equal 'text/plain'
        res.headers['content-length'].should.equal '3'
        res.headers['etag'].should.equal '202cb962ac59075b964b07152d234b70'
        res.headers.should.have.property 'last-modified'
        res.headers.should.have.property 'x-trans-id'
        res.headers.should.have.property 'x-timestamp'

        body.should.equal '123'

    it 'should not get non-existing object info', (done) ->
      HEAD('/test-container/filex').then ([res, body]) ->
        res.statusCode.should.equal 404

    it 'should not get non-existing object', (done) ->
      GET('/test-container/filex').then ([res, body]) ->
        res.statusCode.should.equal 404

    it 'should add and delete object metadata', (done) ->
      headers =
        'x-object-meta-foo': '123'
        'x-object-meta-bar': '456'

      POST('/test-container/file1', headers: headers).then ([res, body]) ->
        res.statusCode.should.equal 202

        HEAD('/test-container/file1').then ([res, body]) ->
          res.headers['x-object-meta-foo'].should.equal '123'
          res.headers['x-object-meta-bar'].should.equal '456'

          headers =
            'x-object-meta-bar': '789'

          POST('/test-container/file1', headers: headers).then ->
            HEAD('/test-container/file1').then ([res, body]) ->
              res.headers.should.not.have.property 'x-object-meta-foo'
              res.headers['x-object-meta-bar'].should.equal '789'

    it 'should fail to add metadata to non-existing object', (done) ->
      headers =
        'x-object-meta-foo': '123'

      POST('/non-existing', headers: headers).then ([res, body]) ->
        res.statusCode.should.equal 404

    it 'should put existing object', (done) ->
      HEAD('/test-container/file1').then ([res, body]) ->
        res.statusCode.should.equal 200
        res.headers['content-type'].should.equal 'text/plain'
        res.headers['content-length'].should.equal '3'
        res.headers['etag'].should.equal '202cb962ac59075b964b07152d234b70'

        opts =
          body: '12345'
          headers:
            'content-type': 'image/jpeg'

        PUT('/test-container/file1', opts).then ([res, body]) ->
          res.statusCode.should.equal 201

          HEAD('/test-container/file1').then ([res, body]) ->
            res.statusCode.should.equal 200
            res.headers['content-type'].should.equal 'image/jpeg'
            res.headers['content-length'].should.equal '5'
            res.headers['etag'].should.equal '827ccb0eea8a706c4c34a16891f84e7b'

    it 'should put new object', (done) ->
      PUT('/test-container/object1', body: 'body').then ([res, body]) ->
        res.statusCode.should.equal 201

        HEAD('/test-container/object1').then ([res, body]) ->
          res.statusCode.should.equal 200

    it 'should put new object with metadata', (done) ->
      headers =
        'x-object-meta-foo': '123'

      PUT('/test-container/object1', body: 'body', headers: headers).then ([res, body]) ->
        res.statusCode.should.equal 201

        HEAD('/test-container/object1').then ([res, body]) ->
          res.headers['x-object-meta-foo'].should.equal '123'

    it 'should put new object with valid etag', (done) ->
      opts =
        headers:
          etag: '841a2d689ad86bd1611447453c22c6fc'
        body: 'body'

      PUT('/test-container/object1', opts).then ([res, body]) ->
        res.statusCode.should.equal 201

    it 'should fail to put new object with invalid etag', (done) ->
      opts =
        headers:
          etag: '841a2d689ad86bd1611447453c22cXXX'
        body: 'body'

      PUT('/test-container/object1', opts).then ([res, body]) ->
        res.statusCode.should.equal 422

    it 'should copy object with put', (done) ->
      PUT('/test-container/object1', body: 'body').then ([res, body]) ->
        res.statusCode.should.equal 201

        headers =
          'x-copy-from': '/test-container/object1'
          'content-type': 'image/jpeg'
          'x-object-meta-foo': '123'

        PUT('/test-container/object1_copy', body: '', headers: headers).then ([res, body]) ->
          res.statusCode.should.equal 201

          HEAD('/test-container/object1').then ([res, body]) ->
            res.statusCode.should.equal 200
            res.headers.should.not.have.property 'x-object-meta-foo'

            HEAD('/test-container/object1_copy').then ([res, body]) ->
              res.statusCode.should.equal 200
              res.headers['x-object-meta-foo'].should.equal '123'
              res.headers['content-type'].should.equal 'image/jpeg'

    it 'should fail to copy non-existing object with put', (done) ->
      headers =
        'x-copy-from': '/test-container/objectx'

      PUT('/test-container/object1_copy', body: '', headers: headers).then ([res, body]) ->
        res.statusCode.should.equal 404

    it 'should fail to copy non-existing container with put', (done) ->
      headers =
        'x-copy-from': '/test-containe/objectx'

      PUT('/test-container/object1_copy', body: '', headers: headers).then ([res, body]) ->
        res.statusCode.should.equal 404

    it 'should fail to copy invalid object with put', (done) ->
      headers =
        'x-copy-from': '/test-container/'

      PUT('/test-container/object1_copy', body: '', headers: headers).then ([res, body]) ->
        res.statusCode.should.equal 400

    it 'should fail to copy invalid path with put', (done) ->
      headers =
        'x-copy-from': '/test'

      PUT('/test-container/object1_copy', body: '', headers: headers).then ([res, body]) ->
        res.statusCode.should.equal 412

    it 'should copy object', (done) ->
      PUT('/test-container/object1', body: 'body').then ([res, body]) ->
        res.statusCode.should.equal 201

        headers =
          'destination': '/test-container/object1_copy'
          'content-type': 'image/jpeg'
          'x-object-meta-foo': '123'

        COPY('/test-container/object1', body: '', headers: headers).then ([res, body]) ->
          res.statusCode.should.equal 201

          HEAD('/test-container/object1').then ([res, body]) ->
            res.statusCode.should.equal 200
            res.headers.should.not.have.property 'x-object-meta-foo'

            HEAD('/test-container/object1_copy').then ([res, body]) ->
              res.statusCode.should.equal 200
              res.headers['x-object-meta-foo'].should.equal '123'
              res.headers['content-type'].should.equal 'image/jpeg'

    it 'should fail to copy non-existing object', (done) ->
      headers =
        'destination': '/test-container/object1_copy'

      COPY('/test-container/objectx', body: '', headers: headers).then ([res, body]) ->
        res.statusCode.should.equal 404

    it 'should fail to copy non-existing dest container', (done) ->
      headers =
        'destination': '/test-containe/object1_copy'

      COPY('/test-container/file1', body: '', headers: headers).then ([res, body]) ->
        res.statusCode.should.equal 404

    it 'should fail to copy invalid dest object', (done) ->
      headers =
        'destination': '/test-container/'

      COPY('/test-container/file1', body: '', headers: headers).then ([res, body]) ->
        res.statusCode.should.equal 503

    it 'should fail to copy invalid dest path', (done) ->
      headers =
        'destination': '/test'

      COPY('/test-container/file1', body: '', headers: headers).then ([res, body]) ->
        res.statusCode.should.equal 412

    it 'should delete object', (done) ->
      PUT('/test-container/object1', body: 'body').then ->
        DELETE('/test-container/object1').then ([res, body]) ->
          res.statusCode.should.equal 204

    it 'should not delete non-existing object', (done) ->
      DELETE('/test-container/non-existing').then ([res, body]) ->
        res.statusCode.should.equal 404

    it 'should put manifest', (done) ->
      createObjects([['parts/part1', '123'], ['parts/part2', '456']]).then ->
        headers =
          'x-object-manifest': 'test-container/parts'
          'content-type': 'image/jpeg'

        PUT('/test-container/all', body: '', headers: headers).then ([res, body]) ->
          res.statusCode.should.equal 201

          GET('/test-container?format=json', json: yes).then ([res, body]) ->
            manifest = _.find body, (x) -> x.name == 'all'

            manifest.name.should.equal 'all'
            manifest.bytes.should.equal 0
            manifest.hash.should.equal 'd41d8cd98f00b204e9800998ecf8427e'
            manifest.content_type.should.equal 'image/jpeg'

            HEAD('/test-container/all').then ([res, body]) ->
              res.statusCode.should.equal 200
              res.headers['content-type'].should.equal 'image/jpeg'
              res.headers['content-length'].should.equal '6'
              res.headers['etag'].should.equal '"79e3c0c96e5d7ccac5763c4f49bfb98d"'
              res.headers.should.have.property 'last-modified'
              res.headers.should.have.property 'x-trans-id'
              res.headers.should.have.property 'x-timestamp'

              GET('/test-container/all').then ([res, body]) ->
                res.statusCode.should.equal 200
                res.headers['content-type'].should.equal 'image/jpeg'
                res.headers['content-length'].should.equal '6'
                res.headers['etag'].should.equal '"79e3c0c96e5d7ccac5763c4f49bfb98d"'
                res.headers.should.have.property 'last-modified'
                res.headers.should.have.property 'x-trans-id'
                res.headers.should.have.property 'x-timestamp'

                body.should.equal '123456'

    it 'should put manifest without files', (done) ->
      headers =
        'x-object-manifest': 'test-container/noparts'
        'content-type': 'image/jpeg'

      PUT('/test-container/all', body: '', headers: headers).then ([res, body]) ->
        res.statusCode.should.equal 201

        HEAD('/test-container/all').then ([res, body]) ->
          res.statusCode.should.equal 200
          res.headers['content-type'].should.equal 'image/jpeg'
          res.headers['content-length'].should.equal '0'

    it 'should put manifest with invalid container', (done) ->
      headers =
        'x-object-manifest': 'nonexistingcontainer/parts'
        'content-type': 'image/jpeg'

      PUT('/test-container/all', body: '', headers: headers).then ([res, body]) ->
        res.statusCode.should.equal 201

        HEAD('/test-container/all').then ([res, body]) ->
          res.statusCode.should.equal 404

    it 'should put manifest with invalid etag', (done) ->
      PUT('/test-container/parts/part1', body: '123').then ->
        PUT('/test-container/parts/part2', body: '456').then ->
          headers =
            'x-object-manifest': 'test-container/parts'
            'content-type': 'image/jpeg'
            'etag': '123123'

          PUT('/test-container/all', body: '', headers: headers).then ([res, body]) ->
            res.statusCode.should.equal 503

    it 'should copy manifest with put', (done) ->
      PUT('/test-container/parts/part1', body: '123').then ->
        PUT('/test-container/parts/part2', body: '456').then ->
          headers =
            'x-object-manifest': 'test-container/parts'
            'content-type': 'image/jpeg'

          PUT('/test-container/all', body: '', headers: headers).then ([res, body]) ->
            res.statusCode.should.equal 201

            headers =
              'x-copy-from': '/test-container/all'
              'content-type': 'image/png'

            PUT('/test-container/all_copy', body: '', headers: headers).then ([res, body]) ->
              res.statusCode.should.equal 201

              HEAD('/test-container/all').then ([res, body]) ->
                res.statusCode.should.equal 200
                res.headers['content-type'].should.equal 'image/jpeg'
                res.headers['content-length'].should.equal '6'

                HEAD('/test-container/all_copy').then ([res, body]) ->
                  res.statusCode.should.equal 200
                  res.headers['content-type'].should.equal 'image/png'
                  res.headers['content-length'].should.equal '6'

    it 'should copy new manifest with put', (done) ->
      PUT('/test-container/parts1/part', body: '123').then ->
        PUT('/test-container/parts2/part', body: '456').then ->
          headers =
            'x-object-manifest': 'test-container/parts1'
            'content-type': 'image/jpeg'

          PUT('/test-container/all', body: '', headers: headers).then ([res, body]) ->
            res.statusCode.should.equal 201

            headers =
              'x-copy-from': '/test-container/all'
              'content-type': 'image/png'
              'x-object-manifest': 'test-container/parts2'

            PUT('/test-container/all_copy', body: '', headers: headers).then ([res, body]) ->
              res.statusCode.should.equal 201

              GET('/test-container/all').then ([res, body]) ->
                res.statusCode.should.equal 200
                res.headers['content-type'].should.equal 'image/jpeg'
                body.should.equal '123'

                GET('/test-container/all_copy').then ([res, body]) ->
                  res.statusCode.should.equal 200
                  res.headers['content-type'].should.equal 'image/png'
                  body.should.equal '456'

    it 'should copy manifest', (done) ->
      PUT('/test-container/parts/part1', body: '123').then ->
        PUT('/test-container/parts/part2', body: '456').then ->
          headers =
            'x-object-manifest': 'test-container/parts'
            'content-type': 'image/jpeg'

          PUT('/test-container/all', body: '', headers: headers).then ([res, body]) ->
            res.statusCode.should.equal 201

            headers =
              'destination': '/test-container/all_copy'
              'content-type': 'image/png'

            COPY('/test-container/all', body: '', headers: headers).then ([res, body]) ->
              res.statusCode.should.equal 201

              HEAD('/test-container/all').then ([res, body]) ->
                res.statusCode.should.equal 200
                res.headers['content-type'].should.equal 'image/jpeg'
                res.headers['content-length'].should.equal '6'

                HEAD('/test-container/all_copy').then ([res, body]) ->
                  res.statusCode.should.equal 200
                  res.headers['content-type'].should.equal 'image/png'
                  res.headers['content-length'].should.equal '6'

    it 'should copy new manifest', (done) ->
      PUT('/test-container/parts1/part', body: '123').then ->
        PUT('/test-container/parts2/part', body: '456').then ->
          headers =
            'x-object-manifest': 'test-container/parts1'
            'content-type': 'image/jpeg'

          PUT('/test-container/all', body: '', headers: headers).then ([res, body]) ->
            res.statusCode.should.equal 201

            headers =
              'destination': '/test-container/all_copy'
              'content-type': 'image/png'
              'x-object-manifest': 'test-container/parts2'

            COPY('/test-container/all', body: '', headers: headers).then ([res, body]) ->
              res.statusCode.should.equal 201

              GET('/test-container/all').then ([res, body]) ->
                res.statusCode.should.equal 200
                res.headers['content-type'].should.equal 'image/jpeg'
                body.should.equal '123'

                GET('/test-container/all_copy').then ([res, body]) ->
                  res.statusCode.should.equal 200
                  res.headers['content-type'].should.equal 'image/png'
                  body.should.equal '456'

  describe 'ACL', ->
    setup()
    auth()
    withContainer()
    withObject()

    it 'should get object with public ACL', (done) ->
      headers = 'x-container-read': '.r:*'

      PUT('/test-container', headers: headers).then ([res, body]) ->
        HEAD('/test-container/file1', noAuth: yes).then ([res, body]) ->
          res.statusCode.should.equal 200

    it 'should not put object with public ACL', (done) ->
      headers = 'x-container-read': '.r:*'

      PUT('/test-container', headers: headers).then ([res, body]) ->
        PUT('/test-container/file1', noAuth: yes).then ([res, body]) ->
          res.statusCode.should.equal 401


chai.Assertion.addMethod 'els', (keys) ->
  arr = @__flags.object
  should.exist arr
  obj = _(arr).map((x) -> [x, true]).object().value()
  obj.should.have.keys keys

chai.Assertion.addMethod 'eqlSorted', (other) ->
  arr = @__flags.object
  should.exist arr
  arr = _.sortBy(arr, (x) -> x)
  other = _.sortBy(other, (x) -> x)
  arr.should.eql other
