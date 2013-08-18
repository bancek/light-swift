#!/usr/bin/env coffee

fs = require('fs')

clean = (x) -> x.replace(/\.\w+$/, '')

backendProviders = fs.readdirSync(__dirname + '/backend').map(clean)
storageProviders = fs.readdirSync(__dirname + '/storage').map(clean)

opts = require('optimist')
  .usage("""
    Usage: $0 [options]

    Backend providers: #{backendProviders.join(', ')}
    Storage providers: #{storageProviders.join(', ')}
    """)

  .describe('h', 'Help')
  .alias('h', 'help')

  .describe('v', 'Verbose')
  .alias('v', 'verbose')
  .boolean('v')

  .describe('p', 'Port to listen on')
  .alias('p', 'port')
  .default('p', 8080)

  .describe('a', 'Swift account (tenant)')
  .alias('a', 'account')
  .default('a', 'test')
  .string('a')

  .describe('u', 'Swift username')
  .alias('u', 'username')
  .default('u', 'tester')
  .string('u')

  .describe('k', 'Swift user key')
  .alias('k', 'key')
  .default('k', 'testing')
  .string('k')

  .describe('c', 'Swift default container')
  .alias('c', 'container')
  .default('c', '')
  .string('c')

  .describe('b', 'Backend')
  .alias('b', 'backend')
  .default('b', 'memory')
  .string('b')

  .describe('t', 'Backend options')
  .alias('t', 'backendopts')
  .default('t', '[]')
  .string('t')

  .describe('s', 'Storage')
  .alias('s', 'storage')
  .default('s', 'memory')
  .string('s')

  .describe('o', 'Storage options')
  .alias('o', 'storageopts')
  .default('o', '[]')
  .string('o')

argv = opts.argv

if argv.help
  opts.showHelp()
  process.exit()

LightSwift = require './light-swift'

Backend = require('./backend/' + argv.backend)
Storage = require('./storage/' + argv.storage)

create = (cls, args) ->
  args.unshift(null)
  factory = cls.bind.apply(cls, args)
  new factory()

options =
  port: argv.port
  verbose: argv.verbose
  storage: create(Storage, JSON.parse(argv.storageopts))
  backend: create(Backend, JSON.parse(argv.backendopts))

swift = new LightSwift(options)

swift.connect()
  .then(-> swift.addAccount(argv.account))
  .then(-> swift.addUser(argv.account, argv.username, argv.key))
  .then(->
    swift.addContainer(argv.account, argv.container) if argv.container
  )
  .then(-> swift.server())
  .then(-> console.log("Light Swift server started on " +
    "0.0.0.0:#{options.port} using #{argv.backend} backend " +
    "and #{argv.storage} storage"))
  .fail((err) -> console.error(err))
