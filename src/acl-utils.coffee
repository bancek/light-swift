url = require('url')

endsWith = (str, suffix) ->
  str.indexOf(suffix, str.length - suffix.length) isnt -1

exports.cleanAcl = (name, value) ->
  name = name.toLowerCase()

  values = []

  value.split(',').forEach (rawValue) ->
    rawValue = rawValue.trim()

    if rawValue
      if not /:/.test(rawValue)
        values.push(rawValue)
      else
        second = rawValue.split(':')
        first = second.shift().trim()
        second = second.join(':').trim()

        if not first or first[0] != '.'
          values.push(rawValue)
        else if first in ['.r', '.ref', '.referer', '.referrer']
          if /write/.test(name)
            throw new Error("Referrers not allowed in write ACL: #{rawValue}")

          negate = no

          if second and second[0] == '-'
            negate = yes
            second = second.slice(1).trim()

          if second and second != '*' and second[0] == '*'
            second = second.slice(1).trim()

          if not second or second == '.'
            throw new Error("No host/domain value after referrer " +
                            "designation in ACL: #{rawValue}")

          negate = if negate then '-' else ''

          values.push(".r:#{negate}#{second}")

        else
          throw new Error("Unknown designator #{first} in ACL: #{rawValue}")

  values.join(',')

exports.parseAcl = (aclString) ->
  referrers = []
  groups = []

  if aclString
    aclString.split(',').forEach (value) ->
      if /^\.r:/.test(value)
        referrers.push(value.substring(3))
      else
        groups.push(value)

  [referrers, groups]

exports.referrerAllowed = (referrer, referrerAcl) ->
  allow = no

  if referrerAcl?.length
    rhost = url.parse(referrer or '').hostname or 'unknown'

    referrerAcl.forEach (mhost) ->
      if mhost[0] == '-'
        mhost = mhost.slice(1)

        if mhost == rhost or (mhost[0] == '.' and endsWith(rhost, mhost))
          allow = no

      else if mhost == '*' or mhost == rhost \
          or (mhost[0] == '.' and endsWith(rhost, mhost))
        allow = yes

  allow
