var util = require('util');
var events = require('events');
var _ = require('underscore');

/**
 * Creates a new DataMerger.
 *
 * @param string[] datasets - A list of identifiers for datasets to be merged.
 *
 * @class
 * @classdesc Merge data from multiple sources into a single dataset.
 */
var DataMerger = exports.DataMerger = function(datasets) {
  // A list of identifiers for datasets that will be merged
  this.datasets = datasets;
  // A map representing the "rows" of merged data.  Keys are a unique
  // record identifier, shared across the different datasets.  
  // Values are objects of the data fields and values.
  this.store = {};
  // A list of record Ids that exist in each dataset. 
  this._idsForSet = {};
  datasets.forEach(function(dataset) {
    this._idsForSet[dataset] = [];
  }, this);
  // Count of total number of merged records
  this.merged = 0;
};
util.inherits(DataMerger, events.EventEmitter);

_.extend(DataMerger.prototype, {
  update: function(dataset, id, data) {
    if (!this.store[id]) {
      this.store[id] = {};
      this.store[id]._count = 0;
    }

    if (!this.store[id][dataset]) {
      this.store[id]._count++;
      this._idsForSet[dataset].push(id);
    }
    this.store[id][dataset] = data;

    this.emit('update', dataset, id, data, this.store[id]._count);

    if (this.store[id]._count == this.datasets.length) {
      this.merged++;
      this.emit('merged', id, this.merge(id)); 
    }
  },

  merge: function(id) {
    var merged = {};
    this.datasets.forEach(function(dataset) {
      _.defaults(merged, this.store[id][dataset]);
    }, this);
    return merged;
  },

  has: function(dataset, id) {
    return !_.isUndefined(this.store[id]) && !_.isUndefined(this.store[id][dataset]);
  },

  get: function(dataset, id) {
    return this.store[id][dataset];
  },

  ids: function(dataset) {
    return this._idsForSet[dataset];
  }
});
