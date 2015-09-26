var Writable = require('stream').Writable;
var domain = require('domain');
var util = require('util');
var elasticsearch = require('elasticsearch');
var moment = require('moment');

// we assume unknown levels should get mapped to the next
// highest known level name
function getLevelName(levels, level) {
  var levelName = levels[level];

  if (!levelName) {
    if (level > 50) {
      levelName = 'fatal';
    } else if (level > 40) {
      levelName = 'error';
    } else if (level > 30) {
      levelName = 'warn';
    } else if (level > 20) {
      levelName = 'info';
    } else if (level > 10) {
      levelName = 'debug';
    } else {
      levelName = 'trace';
    }
  }

  return levelName;
}

function generateIndexName (pattern, entry) {
  return moment.utc(entry.timestamp).format(pattern);
}

function callOrString (value, entry) {
  if (typeof(value) === 'function') {
    return value(entry);
  }
  return value;
}

function ElasticsearchStream (options) {
  options = options || {};
  this._levels = options.levels || {};
  this._client = options.client || new elasticsearch.Client(options);
  this._type = options.type || 'logs';
  var indexPattern = options.indexPattern || '[logstash-]YYYY.MM.DD';
  this._index = options.index || generateIndexName.bind(null, indexPattern);
  Writable.call(this, options);
}

util.inherits(ElasticsearchStream, Writable);

ElasticsearchStream.prototype._write = function (entry, encoding, callback) {

  var client = this._client;
  var index = this._index;
  var type = this._type;
  var levels = this._levels;

  var d = domain.create();
  d.on('error', function (err) {
    console.log("Elasticsearch Error", err.stack);
  });
  d.run(function () {
    entry = JSON.parse(entry.toString('utf8'));
    var env = process.env.NODE_ENV || 'development';

    // Reassign these fields so them match what the default Kibana dashboard
    // expects to see.
    entry['@timestamp'] = entry.time;
    entry.level = getLevelName(levels, entry.level);
    entry.message = entry.msg;

    // remove duplicate fields
    delete entry.time;
    delete entry.msg;

    var datestamp = moment(entry.timestamp).format('YYYY.MM.DD');

    var options = {
      index: callOrString(index, entry),
      type: callOrString(type, entry),
      body: entry
    };

    client.create(options, function (err, resp) {
      if (err) console.log('Elasticsearch Stream Error:', err.stack);
      callback();
    });

  });
};

module.exports = ElasticsearchStream;
