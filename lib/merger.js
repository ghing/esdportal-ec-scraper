var util = require('util');
var events = require('events');
var _ = require('underscore');

/**
 * Creates a new DataMerger.
 *
 * @param string[] datasets - A list of identifiers for datasets to be merged.
 *   Datasets in this list are expected to have the same number of records in
 *   each datasets.
 * @param string[] optionalDatasets - A list of identifiers for datasets to
 *   be merged.  There may not be a record in these datasets for every record
 *   in the other datasets.
 *
 * @class
 * @classdesc Merge data from multiple sources into a single dataset.
 */
var DataMerger = exports.DataMerger = function(datasets, optionalDatasets) {
  // A list of identifiers for datasets that will be merged
  this.datasets = datasets;
  this.optionalDatasets = optionalDatasets; 
  this._hasAllData = {};
  this._hasAllDataOptionalCount = 0;
  this._hasAllDataRequiredCount = 0;
  this._requiredByName = {};
  this._optionalByName = {};
  // A map representing the "rows" of merged data.  Keys are a unique
  // record identifier, shared across the different datasets.  
  // Values are objects of the data fields and values.
  this.store = {};
  // A list of record Ids that exist in each dataset. 
  this._idsForSet = {};
  datasets.forEach(function(dataset) {
    this._idsForSet[dataset] = {};
    this._requiredByName[dataset] = true;
  }, this);
  optionalDatasets.forEach(function(dataset) {
    this._idsForSet[dataset] = {};
    this._optionalByName[dataset] = true;
  }, this);
  // Count of total number of merged records
  this.merged = 0;
};
util.inherits(DataMerger, events.EventEmitter);

_.extend(DataMerger.prototype, {
  update: function(dataset, id, data) {
    if (!this.store[id]) {
      this.store[id] = {};
      this.store[id]._optionalCount = 0;
      this.store[id]._requiredCount = 0;
    }

    if (!this.store[id][dataset]) {
      if (this._requiredByName[dataset]) {
        this.store[id]._requiredCount++;
      }
      else if (this._optionalByName[dataset]) {
        this.store[id]._optionalCount++;
      }
      this._idsForSet[dataset][id] = true;
    }
    this.store[id][dataset] = data;

    this.emit('update', dataset, id, data, this.store[id]._requiredCount,
              this.store[id]._optionalCount);

    if (this.completed(id)) {
      this.merge(id);
    }
  },

  /**
   * Tell the merger that all records for a particular dataset have been
   * processed.
   */
  setHasAllData: function(dataset) {
    // Make a guard to ignore multiple calls with the same dataset
    // argument
    if (!this._hasAllData[dataset]) {
      this._hasAllData[dataset] = true;
      if (this._optionalByName[dataset]) {
        this._hasAllDataOptionalCount++;
      }
      else if (this._requiredByName[dataset]) {
        this._hasAllDataRequiredCount++;
      }

      if (this._hasAllDataOptionalCount == this.optionalDatasets.length) {
        // All data in the optionaldatasets has been processed.
        this._flushCompleted();
      }
    }
  },

  completed: function(id) {
    // To be complete there must be data from all of the required datasets
    if (this.store[id]._requiredCount != this.datasets.length) {
      return false;
    }

    // There's data for all datasets. Complete!
    if (this.store[id]._optionalCount == this.optionalDatasets.length) {
      return true;
    }

    // There's data for the required datasets and we've processed all records
    // in the optional datasets, so there's nothing more to wait for.
    if (this._hasAllDataOptionalCount == this.optionalDatasets.length) {
      return true;
    }

    return false;
  },

  merge: function(id, remove) {
    if (_.isUndefined(remove)) {
      remove = true;
    }

    this.merged++;
    this.emit('merged', id, this.getMergedData(id)); 

    if (remove) {
      this.remove(id);
    }
  },

  getMergedData: function(id) {
    var merged = {};
    this.datasets.forEach(function(dataset) {
      _.defaults(merged, this.store[id][dataset]);
    }, this);
    this.optionalDatasets.forEach(function(dataset) {
      _.defaults(merged, this.store[id][dataset]);
    }, this);
    return merged;
  },

  remove: function(id) {
    delete this.store[id];
    for (var dataset in this._idsForSet) {
      delete this._idsForSet[dataset][id];
    }
  },

  _flushCompleted: function() {
    for (var id in this.store) {
      this.merge(id);
    }
  },

  has: function(dataset, id) {
    return !_.isUndefined(this.store[id]) && !_.isUndefined(this.store[id][dataset]);
  },

  get: function(dataset, id) {
    return this.store[id][dataset];
  },

  ids: function(dataset) {
    var ids = [];
    for (var id in this._idsForSet[dataset]) {
      ids.push(id);
    }
    return ids;
  }
});
