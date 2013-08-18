// Generated by CoffeeScript 1.6.3
(function() {
  var FilesystemBackend, calls, ensureDir, ensureDirs, file, fs, lockFile, name, nodePath, q, track, xattr, xattrAsync, _,
    __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; },
    __slice = [].slice;

  fs = require('fs');

  q = require('q');

  _ = require('lodash');

  nodePath = require('path');

  file = require('file');

  lockFile = require('lockfile');

  xattrAsync = require('xattr-async');

  xattr = {
    list: q.denodeify(xattrAsync.list),
    get: q.denodeify(xattrAsync.get),
    set: q.denodeify(xattrAsync.set),
    remove: q.denodeify(xattrAsync.remove)
  };

  if (process.env.DEBUG) {
    calls = {};
    track = function(name, func) {
      var inner;
      return inner = function() {
        calls[name] += 1;
        return func.apply(func, arguments);
      };
    };
    for (name in xattr) {
      calls[name] = 0;
      xattr[name] = track(name, xattr[name]);
    }
    setInterval(function() {
      return console.log(calls);
    }, 5000);
  }

  ensureDirs = function(path) {
    return q.ninvoke(file, 'mkdirs', path, null).fail(function(e) {
      if ((e != null ? e.code : void 0) !== 'EEXIST') {
        throw e;
      }
    });
  };

  ensureDir = function(path) {
    return q.nfcall(fs.mkdir, path).fail(function(e) {
      if ((e != null ? e.code : void 0) !== 'EEXIST') {
        throw e;
      }
    });
  };

  FilesystemBackend = (function() {
    function FilesystemBackend(basePath) {
      this.setObjectMetadata = __bind(this.setObjectMetadata, this);
      this.getObjects = __bind(this.getObjects, this);
      this.getObject = __bind(this.getObject, this);
      this.deleteObject = __bind(this.deleteObject, this);
      this.setObjectLastModified = __bind(this.setObjectLastModified, this);
      this.addObject = __bind(this.addObject, this);
      this.objectPath = __bind(this.objectPath, this);
      this.objectUnescape = __bind(this.objectUnescape, this);
      this.objectEscape = __bind(this.objectEscape, this);
      this.containerDeleteObject = __bind(this.containerDeleteObject, this);
      this.containerAddObject = __bind(this.containerAddObject, this);
      this.setContainerAcl = __bind(this.setContainerAcl, this);
      this.setContainerMetadata = __bind(this.setContainerMetadata, this);
      this.getContainers = __bind(this.getContainers, this);
      this.getContainer = __bind(this.getContainer, this);
      this.setContainerLastModified = __bind(this.setContainerLastModified, this);
      this.deleteContainer = __bind(this.deleteContainer, this);
      this.addContainer = __bind(this.addContainer, this);
      this.containerPath = __bind(this.containerPath, this);
      this.getAuthTokenAccount = __bind(this.getAuthTokenAccount, this);
      this.addAuthToken = __bind(this.addAuthToken, this);
      this.authTokenPath = __bind(this.authTokenPath, this);
      this.getUser = __bind(this.getUser, this);
      this.addUser = __bind(this.addUser, this);
      this.userPath = __bind(this.userPath, this);
      this.accountDeleteObject = __bind(this.accountDeleteObject, this);
      this.accountAddObject = __bind(this.accountAddObject, this);
      this.accountDeleteContainer = __bind(this.accountDeleteContainer, this);
      this.accountAddContainer = __bind(this.accountAddContainer, this);
      this.setAccountMetadata = __bind(this.setAccountMetadata, this);
      this.getAccount = __bind(this.getAccount, this);
      this.setAccountLastModified = __bind(this.setAccountLastModified, this);
      this.addAccount = __bind(this.addAccount, this);
      this.accountAuthPath = __bind(this.accountAuthPath, this);
      this.accountPath = __bind(this.accountPath, this);
      this.updateMeta = __bind(this.updateMeta, this);
      this.setMeta = __bind(this.setMeta, this);
      this.getMeta = __bind(this.getMeta, this);
      this.xattrSetJson = __bind(this.xattrSetJson, this);
      this.xattrGetJson = __bind(this.xattrGetJson, this);
      this.xattrSetStr = __bind(this.xattrSetStr, this);
      this.xattrGetStr = __bind(this.xattrGetStr, this);
      this.lock = __bind(this.lock, this);
      this.path = __bind(this.path, this);
      this.close = __bind(this.close, this);
      this.connect = __bind(this.connect, this);
      this.basePath = basePath || '/tmp/light-swift/';
    }

    FilesystemBackend.prototype.connect = function() {
      var _this = this;
      return ensureDirs(this.basePath).then(function() {
        return ensureDirs(nodePath.join(_this.basePath, 'data')).then(function() {
          return ensureDirs(nodePath.join(_this.basePath, 'auth')).then(function() {
            return ensureDirs(nodePath.join(_this.basePath, 'authtokens'));
          });
        });
      });
    };

    FilesystemBackend.prototype.close = function() {
      return q();
    };

    FilesystemBackend.prototype.path = function() {
      var args;
      args = 1 <= arguments.length ? __slice.call(arguments, 0) : [];
      args.unshift(this.basePath);
      return nodePath.join.apply(nodePath.join, args);
    };

    FilesystemBackend.prototype.lock = function(name, func) {
      var fileName, opts;
      fileName = this.path(name.replace(/\//g, '$').replace(/\.\./g, '$') + '.lock');
      opts = {
        wait: 1000
      };
      return q.nfcall(lockFile.lock, fileName, opts).then(function() {
        return func().fin(function() {
          return q.nfcall(lockFile.unlock, fileName);
        });
      });
    };

    FilesystemBackend.prototype.xattrGetStr = function(path, name, dest) {
      return xattr.get(path, 'user.' + name).then(function(data) {
        return dest[name] = data;
      });
    };

    FilesystemBackend.prototype.xattrSetStr = function(path, name, src) {
      return xattr.set(path, 'user.' + name, src[name]);
    };

    FilesystemBackend.prototype.xattrGetJson = function(path, name) {
      var _this = this;
      return xattr.get(path, 'user.' + name).then(function(data) {
        return JSON.parse(data);
      });
    };

    FilesystemBackend.prototype.xattrSetJson = function(path, name, data) {
      return xattr.set(path, 'user.' + name, JSON.stringify(data));
    };

    FilesystemBackend.prototype.getMeta = function(path) {
      return this.xattrGetJson(path, 'meta');
    };

    FilesystemBackend.prototype.setMeta = function(path, data) {
      var _this = this;
      return this.lock(path, function() {
        return _this.xattrSetJson(path, 'meta', data);
      });
    };

    FilesystemBackend.prototype.updateMeta = function(path, mutator) {
      var _this = this;
      return this.lock(path, function() {
        return _this.xattrGetJson(path, 'meta').then(function(data) {
          mutator(data);
          return _this.xattrSetJson(path, 'meta', data);
        });
      });
    };

    FilesystemBackend.prototype.accountPath = function(account) {
      return this.path('data', account);
    };

    FilesystemBackend.prototype.accountAuthPath = function(account) {
      return this.path('auth', account);
    };

    FilesystemBackend.prototype.addAccount = function(account, accountInfo) {
      var authPath, path,
        _this = this;
      path = this.accountPath(account);
      authPath = this.accountAuthPath(account);
      return ensureDir(path).then(function() {
        return ensureDir(authPath).then(function() {
          return _this.setMeta(path, {
            bytesUsed: accountInfo.bytesUsed,
            containerCount: accountInfo.containerCount,
            objectCount: accountInfo.objectCount,
            lastModified: accountInfo.lastModified.getTime(),
            metadata: {}
          });
        });
      });
    };

    FilesystemBackend.prototype.setAccountLastModified = function(account, lastModified) {
      var path;
      path = this.accountPath(account);
      return this.updateMeta(path, function(data) {
        return data.lastModified = lastModified.getTime();
      });
    };

    FilesystemBackend.prototype.getAccount = function(account) {
      var path,
        _this = this;
      path = this.accountPath(account);
      return this.getMeta(path).then(function(data) {
        data.lastModified = new Date(data.lastModified);
        return data;
      });
    };

    FilesystemBackend.prototype.setAccountMetadata = function(account, metadata) {
      var path;
      path = this.accountPath(account);
      return this.updateMeta(path, function(data) {
        return data.metadata = metadata;
      });
    };

    FilesystemBackend.prototype.accountAddContainer = function(account) {
      var path;
      path = this.accountPath(account);
      return this.updateMeta(path, function(data) {
        return data.containerCount += 1;
      });
    };

    FilesystemBackend.prototype.accountDeleteContainer = function(account) {
      var path;
      path = this.accountPath(account);
      return this.updateMeta(path, function(data) {
        return data.containerCount -= 1;
      });
    };

    FilesystemBackend.prototype.accountAddObject = function(account, size) {
      var path;
      path = this.accountPath(account);
      return this.updateMeta(path, function(data) {
        data.objectCount += 1;
        return data.bytesUsed += size;
      });
    };

    FilesystemBackend.prototype.accountDeleteObject = function(account, size) {
      var path;
      path = this.accountPath(account);
      return this.updateMeta(path, function(data) {
        data.objectCount -= 1;
        return data.bytesUsed -= size;
      });
    };

    FilesystemBackend.prototype.userPath = function(account, username) {
      return this.path('auth', account, username);
    };

    FilesystemBackend.prototype.addUser = function(account, username, key) {
      var path,
        _this = this;
      path = this.userPath(account, username);
      return q.nfcall(fs.open, path, 'w').then(function(fd) {
        return q.nfcall(fs.close, fd).then(function() {
          return _this.xattrSetStr(path, 'key', {
            key: key
          });
        });
      });
    };

    FilesystemBackend.prototype.getUser = function(account, username) {
      var info, path,
        _this = this;
      path = this.userPath(account, username);
      info = {};
      return q.nfcall(fs.stat, path).then(function() {
        return _this.xattrGetStr(path, 'key', info).then(function() {
          return info;
        });
      }, function(err) {
        return null;
      });
    };

    FilesystemBackend.prototype.authTokenPath = function(authToken) {
      return this.path('authtokens', authToken);
    };

    FilesystemBackend.prototype.addAuthToken = function(account, authToken) {
      var path,
        _this = this;
      path = this.authTokenPath(authToken);
      return q.nfcall(fs.open, path, 'w').then(function(fd) {
        return q.nfcall(fs.close, fd).then(function() {
          return _this.xattrSetStr(path, 'account', {
            account: account
          });
        });
      });
    };

    FilesystemBackend.prototype.getAuthTokenAccount = function(authToken) {
      var info, path,
        _this = this;
      path = this.authTokenPath(authToken);
      info = {};
      return q.nfcall(fs.stat, path).then(function() {
        return _this.xattrGetStr(path, 'account', info).then(function() {
          return info.account;
        });
      }, function(err) {
        return null;
      });
    };

    FilesystemBackend.prototype.containerPath = function(account, container) {
      return this.path('data', account, container);
    };

    FilesystemBackend.prototype.addContainer = function(account, container, containerInfo) {
      var path,
        _this = this;
      path = this.containerPath(account, container);
      return ensureDir(path).then(function() {
        return _this.setMeta(path, {
          bytesUsed: containerInfo.bytesUsed,
          objectCount: containerInfo.objectCount,
          lastModified: containerInfo.lastModified.getTime(),
          metadata: containerInfo.metadata,
          acl: {
            read: null,
            write: null
          }
        });
      });
    };

    FilesystemBackend.prototype.deleteContainer = function(account, container) {
      var path;
      path = this.containerPath(account, container);
      return q.ninvoke(fs, 'rmdir', path);
    };

    FilesystemBackend.prototype.setContainerLastModified = function(account, container, lastModified) {
      var path;
      path = this.containerPath(account, container);
      return this.updateMeta(path, function(data) {
        return data.lastModified = lastModified.getTime();
      });
    };

    FilesystemBackend.prototype.getContainer = function(account, container) {
      var path,
        _this = this;
      path = this.containerPath(account, container);
      return q.nfcall(fs.stat, path).then(function() {
        return _this.getMeta(path).then(function(data) {
          data.lastModified = new Date(data.lastModified);
          return data;
        });
      }, function(err) {
        return null;
      });
    };

    FilesystemBackend.prototype.getContainers = function(account) {
      var path,
        _this = this;
      path = this.accountPath(account);
      return q.nfcall(fs.readdir, path).then(function(res) {
        return q.all(res.map(function(c) {
          return _this.getContainer(account, c);
        })).then(function(infos) {
          return _.zipObject(res, infos);
        });
      });
    };

    FilesystemBackend.prototype.setContainerMetadata = function(account, container, metadata) {
      var path;
      path = this.containerPath(account, container);
      return this.updateMeta(path, function(data) {
        return data.metadata = metadata;
      });
    };

    FilesystemBackend.prototype.setContainerAcl = function(account, container, acl) {
      var path;
      path = this.containerPath(account, container);
      return this.updateMeta(path, function(data) {
        return data.acl = acl;
      });
    };

    FilesystemBackend.prototype.containerAddObject = function(account, container, size) {
      var path;
      path = this.containerPath(account, container);
      return this.updateMeta(path, function(data) {
        data.objectCount += 1;
        return data.bytesUsed += size;
      });
    };

    FilesystemBackend.prototype.containerDeleteObject = function(account, container, size) {
      var path;
      path = this.containerPath(account, container);
      return this.updateMeta(path, function(data) {
        data.objectCount -= 1;
        return data.bytesUsed -= size;
      });
    };

    FilesystemBackend.prototype.objectEscape = function(object) {
      return new Buffer(object).toString('base64').replace(/\//, '.');
    };

    FilesystemBackend.prototype.objectUnescape = function(escaped) {
      return new Buffer(escaped.replace(/\./, '/'), 'base64').toString();
    };

    FilesystemBackend.prototype.objectPath = function(account, container, object) {
      return this.path('data', account, container, this.objectEscape(object));
    };

    FilesystemBackend.prototype.addObject = function(account, container, object, obj) {
      var path,
        _this = this;
      path = this.objectPath(account, container, object);
      return q.nfcall(fs.open, path, 'w').then(function(fd) {
        return q.nfcall(fs.close, fd).then(function() {
          return _this.setMeta(path, {
            object: obj.object,
            contentType: obj.contentType,
            hash: obj.hash,
            contentLength: obj.contentLength,
            lastModified: obj.lastModified.getTime(),
            metadata: obj.metadata
          });
        });
      });
    };

    FilesystemBackend.prototype.setObjectLastModified = function(account, container, object, lastModified) {
      var path;
      path = this.objectPath(account, container, object);
      return this.updateMeta(path, function(data) {
        return data.lastModified = lastModified.getTime();
      });
    };

    FilesystemBackend.prototype.deleteObject = function(account, container, object) {
      var path,
        _this = this;
      path = this.objectPath(account, container, object);
      return this.getObject(account, container, object).then(function(obj) {
        return q.nfcall(fs.unlink, path).then(function() {
          return obj;
        });
      });
    };

    FilesystemBackend.prototype.getObject = function(account, container, object) {
      var info, path,
        _this = this;
      path = this.objectPath(account, container, object);
      info = {
        metadata: {}
      };
      return q.nfcall(fs.stat, path).then(function() {
        return _this.getMeta(path).then(function(data) {
          data.lastModified = new Date(data.lastModified);
          return data;
        });
      }, function(err) {
        return null;
      });
    };

    FilesystemBackend.prototype.getObjects = function(account, container) {
      var path,
        _this = this;
      path = this.containerPath(account, container);
      return q.nfcall(fs.readdir, path).then(function(res) {
        res = res.map(_this.objectUnescape);
        return q.all(res.map(function(o) {
          return _this.getObject(account, container, o);
        })).then(function(infos) {
          return _.zipObject(res, infos);
        });
      });
    };

    FilesystemBackend.prototype.setObjectMetadata = function(account, container, object, metadata) {
      var path;
      path = this.objectPath(account, container, object);
      return this.updateMeta(path, function(data) {
        return data.metadata = metadata;
      });
    };

    return FilesystemBackend;

  })();

  module.exports = FilesystemBackend;

}).call(this);