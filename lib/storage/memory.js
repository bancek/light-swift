// Generated by CoffeeScript 1.6.3
(function() {
  var Duplex, MemoryStorage, q, random32, _ref,
    __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; };

  q = require('q');

  _ref = require('../utils'), random32 = _ref.random32, Duplex = _ref.Duplex;

  MemoryStorage = (function() {
    function MemoryStorage() {
      this.remove = __bind(this.remove, this);
      this.create = __bind(this.create, this);
      this.get = __bind(this.get, this);
      this.objects = {};
    }

    MemoryStorage.prototype.get = function(object, range) {
      var buffer, stream;
      buffer = this.objects[object];
      if (range != null) {
        buffer = buffer.slice(range.start, range.end + 1);
      }
      if (buffer != null) {
        stream = new Duplex();
        stream.write(buffer);
        stream.end();
        return q(stream);
      } else {
        return q.reject(true);
      }
    };

    MemoryStorage.prototype.create = function(stream) {
      var buffers, defer, name,
        _this = this;
      defer = q.defer();
      name = random32();
      buffers = [];
      stream.on('data', function(data) {
        data = new Buffer(data);
        return buffers.push(data);
      });
      stream.on('end', function() {
        var buffer;
        buffer = Buffer.concat(buffers);
        _this.objects[name] = buffer;
        return defer.resolve({
          object: name,
          size: buffer.length
        });
      });
      return defer.promise;
    };

    MemoryStorage.prototype.remove = function(object) {
      delete this.objects[object];
      return q();
    };

    return MemoryStorage;

  })();

  module.exports = MemoryStorage;

}).call(this);