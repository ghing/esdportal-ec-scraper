var vows = require('vows'),
    assert = require('assert');

var DataMerger = require('../lib/merger').DataMerger;

vows.describe('DataMerger').addBatch({
  'A DataMerger instance with partial datasets': {
    topic: function() {
      var topic = this;
      var ds1Data = [
        {
          id: 1,
          value: "abc"
        },
        {
          id: 2,
          value: "def"
        },
        {
          id: 3,
          value: "ghi"
        }
      ];
      var ds2Data = [
        {
          id: 1,
          value: "123"
        },
        {
          id: 2,
          value: "456"
        },
        {
          id: 3,
          value: "789"
        }
      ];
      var dspData = [
        {
          id: 1,
          otherValue: true
        },
        {
          id: 2,
          otherValue: true
        }
      ];
      var datasets = ['ds1', 'ds2'];
      var partialDatasets = ['dsp'];
      var mergedData = {};
      var merger = new DataMerger(datasets, partialDatasets)
        .on('merged', function (id, data) {
          mergedData[id] = data;
          if (merger.merged == 3) {
            topic.callback(null, {
              merger: merger,
              mergedData: mergedData
            });
          }
        });
      var i;
      
      for (i = 0; i < ds1Data.length; i++) {
        merger.update('ds1', ds1Data[i].id, ds1Data[i]);
        merger.update('ds2', ds2Data[i].id, ds2Data[i]);
      }

      for (i = 0; i < dspData.length; i++) {
        merger.update('dsp', dspData[i].id, dspData[i]);
      }
      merger.setHasAllData('dsp');
    },
    'merges partial data': function(err, topic) {
      assert.equal(topic.merger.merged, 3);
      assert.isTrue(topic.mergedData[1].otherValue);
      assert.isTrue(topic.mergedData[2].otherValue);
      assert.isUndefined(topic.mergedData[3].otherValue);
    }
  }
}).export(module);
