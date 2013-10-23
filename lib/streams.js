var util = require('util');
var Transform = require('stream').Transform;

// Response JSON begins with this string, which breaks JSON.parse()
var JSON_PREFIX = /^\)\]\}',/; 

var BAD_ESCAPE = /[\\b]([A-Za-z])/;

var GSCJSONCleanerStream = exports.GSCJSONCleanerStream = function(options) {
  if (!(this instanceof GSCJSONCleanerStream)) {
    return new GSCJSONCleanerStream(options);
  }

  Transform.call(this, options);
  this._cleaned = false;
}
util.inherits(GSCJSONCleanerStream, Transform);

GSCJSONCleanerStream.prototype._transform = function(chunk, encoding, done) {
  if (!this._cleaned) {
    var matched = chunk.toString().match(JSON_PREFIX);
    if (matched && matched.length) {
      this.push(chunk.slice(5));
      this._cleaned = true;
      done();
      return
    }
  }

  // HACK: Escape unescaped backslashes, otherwise JSON parsing
  // breaks
  var srep = chunk.toString();
  matched = srep.match(BAD_ESCAPE); 
  if (matched && matched.length) {
    var newS = srep.replace(BAD_ESCAPE, "\\\\$1");
    this.push(new Buffer(newS));
    done();
    return;
  }

  this.push(chunk);
  done();
};
