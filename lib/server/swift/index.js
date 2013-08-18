// Generated by CoffeeScript 1.6.3
(function() {
  var Hasher, MD5_EMPTY, SwiftServer, express, http, parseRange, q, random32, serverUtils, _, _ref,
    __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; };

  http = require('http');

  q = require('q');

  _ = require('lodash');

  express = require('express');

  parseRange = require('range-parser');

  _ref = require('../../utils'), random32 = _ref.random32, Hasher = _ref.Hasher;

  serverUtils = require('./server-utils');

  MD5_EMPTY = 'd41d8cd98f00b204e9800998ecf8427e';

  SwiftServer = (function() {
    function SwiftServer(swift) {
      this.close = __bind(this.close, this);
      this.listen = __bind(this.listen, this);
      var app, common;
      this.options = swift.options;
      common = function(req, res, next) {
        var authAccount, authToken, getContainer, originalEnd, result;
        if (swift.options.verbose) {
          originalEnd = res.end;
          res.end = function(data) {
            console.log(req.method, req.url, res.statusCode);
            return originalEnd.call(res, data);
          };
        }
        res.lines = function(list) {
          res.set('Content-Type', 'text/plain; charset=utf-8');
          return res.send(list.map(function(x) {
            return x + '\n';
          }).join(''));
        };
        res.set('X-Trans-Id', 'tx' + random32());
        res.timestamp = function(date) {
          if (date == null) {
            date = new Date();
          }
          return res.set('X-Timestamp', date.getTime() / 1000);
        };
        req.json = serverUtils.reqIsJson(req);
        req.head = req.method === 'HEAD';
        req.account = req.param('account');
        req.container = req.param('container');
        req.object = req.param(0);
        authToken = req.get('x-auth-token');
        authAccount = authToken ? swift.getAuthTokenAccount(authToken) : q(null);
        getContainer = function() {
          return swift.getContainer(req.account, req.container);
        };
        result = serverUtils.authorize(req, res, authAccount, getContainer).then(function() {
          return next();
        });
        return result.fail(function(err) {
          if (err.statusCode == null) {
            if (process.env.DEBUG) {
              process.nextTick(function() {
                console.dir(err);
                throw err;
              });
            } else {
              console.error(err);
              console.error(err.stack);
            }
            return res.send(500);
          }
        });
      };
      app = express();
      app.configure(function() {
        app.disable('x-powered-by');
        return app.use(app.router);
      });
      app.get('/auth/v1.0*', function(req, res) {
        var key, user;
        user = req.get('x-auth-user');
        key = req.get('x-auth-key');
        return swift.authenticate(user, key).then(function(authenticated) {
          var account;
          if (authenticated) {
            account = user.split(':')[0];
            return swift.newAuthToken(account).then(function(authToken) {
              var host;
              host = req.get('host');
              res.set('X-Storage-Url', "http://" + host + "/v1/AUTH_" + account);
              res.set('X-Auth-Token', authToken);
              res.set('X-Storage-Token', authToken);
              return res.send(200);
            });
          } else {
            return res.send(401);
          }
        }).fail(function(err) {
          console.error(err);
          console.error(err.stack);
          return res.send(500);
        });
      });
      app.get('/v1/AUTH_:account', common, function(req, res) {
        return swift.getAccount(req.account).then(function(accountInfo) {
          return swift.getContainers(req.account).then(function(containerInfos) {
            var containers;
            containers = _.pairs(containerInfos).map(function(_arg) {
              var info, name;
              name = _arg[0], info = _arg[1];
              return {
                name: name,
                count: info.objectCount,
                bytes: info.bytesUsed
              };
            });
            res.set('X-Account-Bytes-Used', accountInfo.bytesUsed);
            res.set('X-Account-Container-Count', accountInfo.containerCount);
            res.set('X-Account-Object-Count', accountInfo.objectCount);
            res.timestamp(accountInfo.lastModified);
            serverUtils.attachMetadata(accountInfo.metadata, res, 'account');
            if (req.param('marker') != null) {
              containers = [];
            }
            if (req.head) {
              return res.send(204);
            }
            if (req.json) {
              return res.json(containers);
            } else {
              if (containers.length === 0) {
                res.statusCode = 204;
              }
              return res.lines(containers.map(function(x) {
                return x.name;
              }));
            }
          });
        });
      });
      app.post('/v1/AUTH_:account', common, function(req, res) {
        return swift.getAccount(req.account).then(function(accountInfo) {
          var md;
          md = serverUtils.extractMetadata(req.headers, 'account');
          return swift.mergeAccountMetadata(req.account, md).then(function() {
            return res.send(204);
          });
        });
      });
      app.put('/v1/AUTH_:account', common, function(req, res) {
        return res.send(403);
      });
      app["delete"]('/v1/AUTH_:account', common, function(req, res) {
        return res.send(403);
      });
      app.get('/v1/AUTH_:account/:container', common, function(req, res) {
        return swift.getContainer(req.account, req.container).then(function(containerInfo) {
          if (containerInfo == null) {
            return res.send(404);
          }
          return swift.getObjects(req.account, req.container).then(function(objects) {
            var delimiter, marker, objsMeta, path, prefix;
            res.set('X-Container-Bytes-Used', containerInfo.bytesUsed);
            res.set('X-Container-Object-Count', containerInfo.objectCount);
            res.timestamp(containerInfo.lastModified);
            serverUtils.attachMetadata(containerInfo.metadata, res, 'container');
            serverUtils.attachAcl(containerInfo.acl, res);
            if (req.head) {
              return res.send(204);
            }
            prefix = req.param('prefix');
            delimiter = req.param('delimiter');
            path = req.param('path');
            marker = req.param('marker');
            objsMeta = serverUtils.formatObjects(objects, prefix, delimiter, path, marker);
            if (req.json) {
              return res.json(objsMeta);
            } else {
              if (objsMeta.length === 0) {
                res.statusCode = 204;
              }
              return res.lines(_.map(objsMeta, function(x) {
                return x.name || x.subdir;
              }));
            }
          });
        });
      });
      app.put('/v1/AUTH_:account/:container', common, function(req, res) {
        return swift.getContainer(req.account, req.container).then(function(containerInfo) {
          var acl, md;
          acl = serverUtils.extractAcl(req.headers);
          if (containerInfo == null) {
            md = serverUtils.extractMetadata(req.headers, 'container');
            return swift.addContainer(req.account, req.container, md, acl).then(function() {
              return res.send(201);
            });
          } else {
            return swift.mergeContainerAcl(req.account, req.container, acl).then(function() {
              return res.send(202);
            });
          }
        });
      });
      app.post('/v1/AUTH_:account/:container', common, function(req, res) {
        return swift.getContainer(req.account, req.container).then(function(containerInfo) {
          var acl, md;
          if (containerInfo != null) {
            md = serverUtils.extractMetadata(req.headers, 'container');
            acl = serverUtils.extractAcl(req.headers);
            return swift.mergeContainerMetadata(req.account, req.container, md).then(function() {
              return swift.mergeContainerAcl(req.account, req.container, acl).then(function() {
                return res.send(204);
              });
            });
          } else {
            return res.send(404);
          }
        });
      });
      app["delete"]('/v1/AUTH_:account/:container', common, function(req, res) {
        return swift.getContainer(req.account, req.container).then(function(containerInfo) {
          if (containerInfo != null) {
            if (swift.canDeleteContainer(containerInfo)) {
              return swift.deleteContainer(req.account, req.container).then(function() {
                return res.send(204);
              });
            } else {
              return res.send(409);
            }
          } else {
            return res.send(404);
          }
        });
      });
      app.get('/v1/AUTH_:account/:container/*', common, function(req, res) {
        return swift.getObject(req.account, req.container, req.object).then(function(obj) {
          var container, manifest, manifestParts, segments;
          if (obj == null) {
            return res.send(404);
          }
          res.set('Accept-Ranges', 'bytes');
          res.set('Content-Type', obj.contentType);
          res.set('Last-Modified', obj.lastModified.toUTCString());
          res.set('Etag', obj.hash);
          res.timestamp(obj.lastModified);
          serverUtils.attachMetadata(obj.metadata, res);
          segments = null;
          if (obj.objectManifest) {
            manifestParts = obj.objectManifest.split('/');
            container = manifestParts.shift();
            manifest = manifestParts.join('/');
            segments = swift.getContainer(req.account, container).then(function(containerInfo) {
              if (!containerInfo) {
                throw res.send(404);
              }
              return swift.getObjects(req.account, container).then(function(objects) {
                var etag, hashes, manifestLength, segs;
                res.set('X-Object-Manifest', obj.objectManifest);
                manifestLength = manifest.length;
                segs = _(objects).pairs().filter(function(_arg) {
                  var name, seg;
                  name = _arg[0], seg = _arg[1];
                  return name.indexOf(manifest) === 0;
                }).sortBy(function(_arg) {
                  var name, seg;
                  name = _arg[0], seg = _arg[1];
                  return name.slice(manifestLength);
                }).map(function(_arg) {
                  var name, seg;
                  name = _arg[0], seg = _arg[1];
                  return seg;
                });
                hashes = segs.map(function(x) {
                  return x.hash;
                });
                etag = Hasher.hashArray(hashes);
                res.set('Etag', '"' + etag + '"');
                return segs;
              });
            });
          } else {
            segments = q([obj]);
          }
          return segments.then(function(segments) {
            var contentLength, currentLength, next, offset, parsedRange, range;
            contentLength = segments.map(function(x) {
              return x.contentLength;
            }).reduce((function(x, y) {
              return x + y;
            }), 0);
            if (req.head) {
              res.set('Content-Length', contentLength);
              return res.end();
            }
            range = {
              start: 0,
              end: contentLength - 1
            };
            if (req.headers.range) {
              parsedRange = parseRange(contentLength, req.headers.range);
              if (parsedRange === -1) {
                res.set('Content-Range', 'bytes */' + contentLength);
                return res.send(416);
              }
              if (parsedRange !== -2) {
                range = {
                  start: parsedRange[0].start,
                  end: parsedRange[0].end
                };
                res.statusCode = 206;
                res.set('Content-Range', ("bytes " + range.start + "-") + ("" + range.end + "/" + contentLength));
              }
            }
            currentLength = range.end - range.start + 1;
            res.set('Content-Length', currentLength);
            offset = 0;
            next = function() {
              var segment, segmentLength, segmentRange;
              segment = segments.shift();
              if (segment != null) {
                segmentLength = segment.contentLength;
                if (range.start <= (offset + segmentLength) && range.end >= offset) {
                  segmentRange = {
                    start: Math.max(range.start - offset, 0),
                    end: Math.min(range.end - offset, segmentLength - 1)
                  };
                  return swift.objectStream(segment, segmentRange).then(function(stream) {
                    stream.pipe(res, {
                      end: false
                    });
                    return stream.on('end', function() {
                      stream.unpipe(res);
                      offset += segmentLength;
                      return next();
                    });
                  });
                } else {
                  offset += segmentLength;
                  return next();
                }
              } else {
                return res.end();
              }
            };
            return next();
          });
        });
      });
      app.put('/v1/AUTH_:account/:container/*', common, function(req, res) {
        var ci, copyFrom, metadata, obj;
        metadata = serverUtils.extractMetadata(req.headers);
        copyFrom = req.get('x-copy-from');
        if (req.get('content-length') === '0' && copyFrom) {
          ci = serverUtils.parseCopyPath(copyFrom);
          if (ci == null) {
            return res.send(412);
          }
          if (!ci.object) {
            return res.send(400);
          }
          return swift.getObject(req.account, ci.container, ci.object).then(function(obj) {
            if (obj == null) {
              return res.send(404);
            }
            obj = _.cloneDeep(obj);
            if (req.get('content-type')) {
              obj.contentType = req.get('content-type');
            }
            if (req.get('x-object-manifest')) {
              obj.objectManifest = req.get('x-object-manifest');
            }
            if (_.keys(metadata).length) {
              obj.metadata = metadata;
            }
            return swift.copyObject(req.account, req.container, req.object, obj).then(function(obj) {
              res.set('Etag', obj.hash);
              return res.send(201);
            });
          });
        }
        obj = {
          contentType: req.get('content-type'),
          objectManifest: req.get('x-object-manifest'),
          hash: req.get('etag')
        };
        if (_.keys(metadata).length) {
          obj.metadata = metadata;
        }
        if (obj.objectManifest && obj.hash && obj.hash !== MD5_EMPTY) {
          return res.send(503);
        }
        return swift.createObject(req.account, req.container, req.object, obj, req).then(function(obj) {
          res.set('Etag', obj.hash);
          return res.send(201);
        }).fail(function(err) {
          if (err instanceof swift.BadHashError) {
            return res.send(422);
          } else {
            throw err;
          }
        });
      });
      app.copy('/v1/AUTH_:account/:container/*', common, function(req, res) {
        return swift.getObject(req.account, req.container, req.object).then(function(obj) {
          var copyInfo,
            _this = this;
          if (obj == null) {
            return res.send(404);
          }
          if (!req.get('destination')) {
            return res.send(412);
          }
          copyInfo = serverUtils.parseCopyPath(req.get('destination'));
          if (copyInfo == null) {
            return res.send(412);
          }
          if (!copyInfo.object) {
            return res.send(503);
          }
          return swift.getContainer(req.account, copyInfo.container).then(function(containerInfo) {
            var metadata;
            if (containerInfo == null) {
              return res.send(404);
            }
            metadata = serverUtils.extractMetadata(req.headers);
            obj = _.cloneDeep(obj);
            if (req.get('content-type')) {
              obj.contentType = req.get('content-type');
            }
            if (req.get('x-object-manifest')) {
              obj.objectManifest = req.get('x-object-manifest');
            }
            if (_.keys(metadata).length) {
              obj.metadata = metadata;
            }
            return swift.copyObject(req.account, copyInfo.container, copyInfo.object, obj).then(function() {
              return res.send(201);
            });
          });
        });
      });
      app.post('/v1/AUTH_:account/:container/*', common, function(req, res) {
        var md;
        md = serverUtils.extractMetadata(req.headers);
        return swift.setObjectMetadata(req.account, req.container, req.object, md).then(function() {
          return res.send(202);
        });
      });
      app["delete"]('/v1/AUTH_:account/:container/*', common, function(req, res) {
        return swift.getObject(req.account, req.container, req.object).then(function(obj) {
          if (obj != null) {
            return swift.deleteObject(req.account, req.container, req.object).then(function() {
              return res.send(204);
            });
          } else {
            return res.send(404);
          }
        });
      });
      this.app = app;
    }

    SwiftServer.prototype.listen = function() {
      this.httpServer = http.createServer(this.app);
      return this.httpServer.listen(this.options.port);
    };

    SwiftServer.prototype.close = function() {
      var _ref1;
      return (_ref1 = this.httpServer) != null ? _ref1.close() : void 0;
    };

    return SwiftServer;

  })();

  module.exports = SwiftServer;

}).call(this);