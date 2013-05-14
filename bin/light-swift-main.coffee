#!/usr/bin/env coffee

opts = require('optimist')
  .usage("Usage: $0 -v -p [port] -t [tempdir] -a [account] -u [username] -c [container]")

  .describe('h', 'Help')
  .alias('h', 'help')
  
  .describe('v', 'Verbose')
  .alias('v', 'verbose')
  .boolean('v')

  .describe('p', 'Port to listen on')
  .alias('p', 'port')
  .default('p', 8080)

  .describe('t', 'Temporary dir for files (memory if not specified)')
  .alias('t', 'tempdir')

  .describe('a', 'Swift account (tenant)')
  .alias('a', 'account')
  .default('a', 'test')

  .describe('u', 'Swift username')
  .alias('u', 'username')
  .default('u', 'tester')

  .describe('k', 'Swift user key')
  .alias('k', 'key')
  .default('k', 'testing')

  .describe('c', 'Swift default container')
  .alias('c', 'container')
  .default('c', '')
  .string('c')

argv = opts.argv

if argv.help
  opts.showHelp()
  process.exit()

LightSwift = require '../lib/light-swift'

options =
  port: argv.port
  tempDir: argv.tempdir
  verbose: argv.verbose

swift = new LightSwift(options)

swift.addAccount argv.account

swift.addUser argv.account, argv.username, argv.key

if argv.container
  swift.addContainer argv.account, argv.container

swift.authTokensStore['AUTH_tk2540a0a80006f88749929a72bc7f5d11'] = 'test'

swift.server()

console.log "Light Swift server started on 0.0.0.0:#{options.port}"
