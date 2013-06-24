should = require('chai').should()
require('mocha-as-promised')()

{cleanAcl, parseAcl, referrerAllowed} = require('../src/acl-utils')

describe 'ACL', ->
  it 'should clean ACL string', ->
    cleanAcl('read', '.r:*')
      .should.equal('.r:*')
    cleanAcl('read', '.r:*,.r:-.thief.com')
      .should.equal('.r:*,.r:-.thief.com')
    cleanAcl('read', '.r:*.example.com')
      .should.equal('.r:.example.com')
    cleanAcl('read', '.r:*,.r:.example.com,.r:-thief.example.com')
      .should.equal('.r:*,.r:.example.com,.r:-thief.example.com')
    cleanAcl('read', '.r:*,.r:-.thief.com,bobs_account,sues_account:sue')
      .should.equal('.r:*,.r:-.thief.com,bobs_account,sues_account:sue')
    cleanAcl('read', 'bobs_account,sues_account:sue')
      .should.equal('bobs_account,sues_account:sue')
    (-> cleanAcl('read', '.r:.'))
      .should.throw()
    (-> cleanAcl('read', '.foo:*'))
      .should.throw()

    cleanAcl('write', 'bobs_account,sues_account:sue')
      .should.equal('bobs_account,sues_account:sue')
    (-> cleanAcl('write', '.r:*'))
      .should.throw()

    cleanAcl('read', 'bob, sue').should.equal('bob,sue')
    cleanAcl('read', 'bob , sue').should.equal('bob,sue')
    cleanAcl('read', 'bob,,,sue').should.equal('bob,sue')
    cleanAcl('read', '.referrer : *').should.equal('.r:*')
    cleanAcl('read', '.ref:*.example.com').should.equal('.r:.example.com')
    cleanAcl('read', '.r:*, .rlistings').should.equal('.r:*,.rlistings')

  it 'should parse ACL string', ->
    parseAcl('.r:*,.r:-.thief.com,bobs_account,sues_account:sue')
      .should.eql [
        ['*', '-.thief.com'],
        ['bobs_account', 'sues_account:sue']
      ]

  it 'should allow referrer', ->
    refs = ['*']
    referrerAllowed('http://www.example.com', refs).should.be.true

    refs = ['*', '.example.com', '-thief.example.com']
    referrerAllowed('http://www.foo.com', refs).should.be.true
    referrerAllowed('http://example.com', refs).should.be.true
    referrerAllowed('http://www.example.com', refs).should.be.true
    referrerAllowed('http://thief.example.com', refs).should.be.false

    refs = ['*', '-.thiefs.example.com']
    referrerAllowed('http://www.foo.com', refs).should.be.true
    referrerAllowed('http://thiefs.example.com', refs).should.be.true
    referrerAllowed('http://sub.thiefs.example.com', refs).should.be.false

    refs = ['friend.example.com']
    referrerAllowed('http://www.foo.com', refs).should.be.false
    referrerAllowed('http://friend.example.com', refs).should.be.true
    referrerAllowed('http://mutual.friend.example.com', refs).should.be.false

    refs = ['.friends.example.com']
    referrerAllowed('http://www.foo.com', refs).should.be.false
    referrerAllowed('http://friends.example.com', refs).should.be.false
    referrerAllowed('http://mutual.friends.example.com', refs).should.be.true
