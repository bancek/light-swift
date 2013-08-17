_ = require('lodash')
aclUtils = require('../../acl-utils')

checkAcl = (getContainer, authAccount, method, referrer) ->
  getContainer().then (ci) ->
    if ci?
      acl = if ci.acl?.read and method in ['GET', 'HEAD']
        ci.acl.read
      else if ci.acl?.write and method in ['PUT', 'POST', 'DELETE']
        ci.acl.write

      if acl
        [refs, groups] = aclUtils.parseAcl(acl)

        return authAccount in groups or aclUtils.referrerAllowed(referrer, refs)

exports.authorize = (req, res, authAccount, getContainer) ->
  authAccount.then (authAccount) ->
    if authAccount and req.account == authAccount
      return yes

    otherwise = ->
      if not authAccount
        throw res.send 401

      throw res.send 403

    if req.container
      return checkAcl(getContainer, authAccount, \
          req.method, req.headers['referrer']).then (allowed) ->
        if not allowed
          otherwise()

    otherwise()

exports.extractMetadata = (headers, type) ->
  type = 'object' if not type?
  regex = new RegExp("x-#{type}-meta-")
  removeRegex = new RegExp("x-remove-#{type}-meta-")

  md = _(headers).pairs()
    .filter(([k, v]) -> regex.test(k))
    .map(([k, v]) -> [k.replace(regex, ''), v])
    .object().value()

  _(headers).keys()
    .filter((k) -> removeRegex.test(k))
    .map((k) -> k.replace(removeRegex, ''))
    .forEach (k) ->
      md[k] = null

  md

exports.attachMetadata = (metadata, res, type) ->
  type = 'object' if not type?

  _(metadata).pairs().forEach ([k, v]) ->
    if v
      res.set "x-#{type}-meta-#{k}", v

exports.extractAcl = (headers) ->
  acl = {}

  read = headers['x-container-read']
  acl.read = aclUtils.cleanAcl('read', read) if read

  write = headers['x-container-write']
  acl.write = aclUtils.cleanAcl('write', write) if write

  acl

exports.attachAcl = (acl, res) ->
  if acl?.read
    res.set 'x-container-read', acl.read

  if acl?.write
    res.set 'x-container-write', acl.write

exports.formatObjects = (objects, prefix, delimiter, path, marker) ->
  objs = _.keys objects

  pathMode = no

  if path?
    path = path + '/' if path.slice(-1) != '/' and path != ''

    prefix = path
    delimiter = '/'
    pathMode = yes

  if prefix
    prefixRegex = new RegExp('^' + prefix)

    objs = _.filter objs, (obj) -> prefixRegex.test(obj)

  if delimiter
    pfx = prefix or ''

    objs = _.map objs, (obj) ->
      obj = obj.substring(pfx.length)
      parts = obj.split(delimiter)

      if parts.length == 1
        pfx + parts[0]
      else
        pfx + parts[0] + delimiter

    objs = _.uniq objs

  objsMeta = objs.map (objName) ->
    obj = objects[objName]

    if obj?
      bytes: obj.contentLength,
      content_type: obj.contentType,
      hash: obj.hash,
      last_modified: obj.lastModified.toISOString().slice(0, -1),
      name: objName
    else
      subdir: objName

  if pathMode
    objsMeta = objsMeta.filter (x) -> not x.subdir?

  if marker?
    objsMeta = []

  objsMeta

exports.parseCopyPath = (path) ->
  regex = new RegExp('/?([^/]+)/(.*)')

  parts = path.match(regex)

  return if not parts

  container: parts[1]
  object: parts[2]

exports.reqIsJson = (req) ->
  accept = req.get('accept')
  contentType = req.get('content-type')
  format = req.param('format')

  if accept == 'application/json'
    return yes

  if accept == 'text/json'
    return yes

  if contentType? and contentType.split(';')[0] == 'application/json'
    return yes

  if contentType? and contentType.split(';')[0] == 'text/json'
    return yes

  if format == 'json'
    return yes

  no
