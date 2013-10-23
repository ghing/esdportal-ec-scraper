var util = require('util');
var csv = require('csv');
var events = require('events');
var _ = require('underscore');
var JSONStream = require('JSONStream');
var fs = require('fs');
var cheerio = require('cheerio');
var request = require('request').defaults({
  jar: true,
  timeout: 100000
});
var GSCJSONCleanerStream = require('./streams').GSCJSONCleanerStream;

// From http://www.michigan.gov/documents/sos/County_Codes_List_225306_7.pdf
var COUNTY_CODES = {
  82: 'Wayne',
  50: 'Maycomb',
  63: 'Oakland'
};

var FIELDS = exports.FIELDS = [
    'BusinessName',
    'LicenseNumber',
    //'PrimaryQualityContact',
    'Address',
    'City',
    'ZipCode',
    'LicenseType',
    'County',
    'SASRating',
    'SASStatus',
    'PublishedRating',
    'Phone',
    'Website',
    'AgeFrom',
    'AgeTo',
    'MonthsOfOperation',
    'SundayDayOpen',
    'SundayDayClose',
    'MondayDayOpen',
    'MondayDayClose',
    'TuesdayDayOpen',
    'TuesdayDayClose',
    'WednesdayDayOpen',
    'WednesdayDayClose',
    'ThursdayDayOpen',
    'ThursdayDayClose',
    'FridayDayOpen',
    'FridayDayClose',
    'SaturdayDayOpen',
    'SaturdayDayClose',
    'Transportation',
    'Meals',
    'Lat',
    'Lng',
    'GSCProviderID',
    'GSCLicensedDate',
    'GSCpts',
    'GSCptsStaff',
    'GSCptsFamily',
    'GSCptsAdmin',
    'GSCptsEnv',
    'GSCptsCurr',
    'GSCeligibility',
    'GSCspecial',
    'GSCsubsidy',
    'GSCcapacity',
    'GSCcontract',
    'GSCfee',
    'GSCEmail',
    'GSCcontact'
];

var OBJ_TEMPLATE = {};
_.each(FIELDS, function(field) {
  OBJ_TEMPLATE[field] = undefined;
});

var gscAuthenticated = false;
function gscAuthenticate(username, password, callback) {
    var startUrl = 'https://v3.childcareresource.com/turbo/index.cfm'
    var qs = '?username=' + username + '&password=' + password; 
    var authUrl = 'https://v3.childcareresource.com/turbo/index.cfm?action=auth.authRegion'; 
    // Make a request to get the CFID and CFTOKEN cookies
    request.get({
      url: startUrl + qs
    }, function(error, response, body) {
      request.post(authUrl, {
        url: authUrl,
        body: {
          username: username,
          password: password
        },
        json: true
      }, function(error, response, body) {
        gscAuthenticated = true;
        callback(arguments)
      });
    }); 
}

var DataMerger = exports.DataMerger = function(slugs) {
  this.slugs = slugs;
  this.store = {};
  this.merged = 0;
};
util.inherits(DataMerger, events.EventEmitter);

_.extend(DataMerger.prototype, {
  update: function(slug, key, data) {
    if (!this.store[key]) {
      this.store[key] = {};
      this.store[key]._count = 0;
    }

    this.store[key][slug] = data;
    this.store[key]._count++;

    this.emit('update', slug, key, data, this.store[key]._count);

    if (this.store[key]._count == this.slugs.length) {
      this.merged++;
      this.emit('merged', key, this.merge(key)); 
    }
  },

  merge: function(key) {
    var merged = {};
    this.slugs.forEach(function(slug) {
      _.defaults(merged, this.store[key][slug]);
    }, this);
    return merged;
  },

  has: function(slug, key) {
    return !_.isUndefined(this.store[key]) && !_.isUndefined(this.store[key][slug]);
  },

  get: function(slug, key) {
    return this.store[key][slug];
  }
});


var EarlyChildhoodDataset = exports.EarlyChildhoodDataset = function() {};
util.inherits(EarlyChildhoodDataset, events.EventEmitter);

_.extend(EarlyChildhoodDataset.prototype, {
  fields: FIELDS, 

  convert: function(field, record) {
    crosswalk = this.crosswalk[field];
    if (_.isFunction(crosswalk)) {
      return crosswalk(record);
    }

    return record[crosswalk];
  },

  filter: function(data) {
    return data;
  },

  createRecord: function(rawRecord) {
    var record = _.clone(OBJ_TEMPLATE); 
    _.each(this.fields, function(field) {
      record[field] = this.convert(field, rawRecord);
    }, this);
    return record;
  },

  getArchivePath: function(fileName) {
    return './scraped_data/' + fileName;
  }
});

var LICENSE_TYPES = {
  DF: 'Family Home',
  DG: 'Group Home',
  DC: 'Center'
};

var CDCDataset = exports.CDCDataset = function() {};
util.inherits(CDCDataset, EarlyChildhoodDataset);

_.extend(CDCDataset.prototype, {
  fields: [
    'BusinessName',
    'LicenseNumber',
    'Address',
    'City',
    'ZipCode',
    'LicenseType',
    'County',
    'Phone',
    'AgeFrom',
    'AgeTo',
    'MonthsOfOperation'
  ],

  crosswalk: {
    BusinessName: 4, 
    Address: function(row) {
      var addr = row[5];
      var addr2 = row[6];

      return addr2 ? addr + " " + addr2 : addr;
    },
    City: 7,
    ZipCode: function(row) {
      var zip = row[9];
      return zip.length > 5 ? zip.slice(0, 5) : zip;
    },
    LicenseType: function(row) {
      return LICENSE_TYPES[row[11]];
    },
    LicenseNumber: 0,
    City: 7,
    County: function(record) {
      return COUNTY_CODES[record[12]];
    },
    Phone: 10,
    AgeFrom: 14,
    AgeTo: 15,
    MonthsOfOperation: 16 
  },

  filter: function(record) {
    var county = this.convert('County', record);
    return county == 'Wayne' || county == 'Maycomb' || county == 'Okland' ? record : null;
  },

  url: 'http://www.dleg.state.mi.us/fhs/brs/txt/cdc.txt',

  bindStreamEvents: function(stream, options) {
    var dataset = this;
    var options = options || {};
    var filter = _.bind(options.filter || this.filter, this);

    stream
    .transform(function(row, index, callback){
      var filteredRow = filter(row);

      if (_.isNull(filteredRow)) {
        callback(null, null);
        return;
      }

      callback(null, dataset.createRecord(row));
    })
    .on('record', function(record, index) {
      dataset.emit('data', record); 
    })
    .on('error', function(err) {
      console.log(err);
    })
    .on('end', function() {
      dataset.emit('end');
    });

    return this;
  },

  fromFile: function(fileName, options) {
    return this.bindStreamEvents(csv().from.path(fileName), options);
  },

  fromURL: function(url, options) {
    url = url || this.url;
    var writer = fs.createWriteStream(this.getArchivePath('cdc.txt'));
    var req = request.get(url);
    req.pipe(writer);
    return this.bindStreamEvents(csv().from.stream(req), options);
  }
});

var BASE_CRITERIA = exports.BASE_CRITERIA = {
  SEARCHTYPE: "",
  ADDRESS: "",
  CITY: "",
  STATE: "",
  ZIP: "",
  SCHOOL: "",
  SORTBY: "name",
  ADDR: "",
  OPTIONS: {
    "18736": [],
    "18738": [],
    "18743": [],
    "18804": []
  },
  // 1970 = Registered Family Homes
  // 1969 = Licensed Group Homes
  // 1972 = Licensed Centers
  // 2110 = Preschools
  CATEGORIES: "1970,1969,1972,2110"
};


var GSCQuicksearchDataset = exports.GSCQuicksearchDataset = function() {};
util.inherits(GSCQuicksearchDataset, EarlyChildhoodDataset);
GSCQuicksearchDataset._authenticated = false;

_.extend(GSCQuicksearchDataset.prototype, {
  fields: [
    'LicenseNumber',
    'PublishedRating',
    'Website',
    'SundayDayOpen',
    'SundayDayClose',
    'MondayDayOpen',
    'MondayDayClose',
    'TuesdayDayOpen',
    'TuesdayDayClose',
    'WednesdayDayOpen',
    'WednesdayDayClose',
    'ThursdayDayOpen',
    'ThursdayDayClose',
    'FridayDayOpen',
    'FridayDayClose',
    'SaturdayDayOpen',
    'SaturdayDayClose',
    'Lat',
    'Lng',
    'GSCProviderID',
    'GSCLicensedDate',
    'GSCEmail'
  ],

  crosswalk: {
    BusinessName: 'NAME',
    LicenseNumber: 'LICENSEID',
    Address: 'WLSFIELD4',
    City: 'WLSFIELD5',
    ZipCode: 'WLSFIELD7',
    LicenseType: 'PROVIDERTYPES',
    PublishedRating: 'STARS',
    Phone: 'WLSFIELD9',
    Website: 'FIELD18704',
    AgeFrom: 'YOUNGESTAGE',
    AgeTo: 'OLDESTAGE',
    SundayDayOpen: 'SundayDayOpen',
    SundayDayClose: 'SundayDayClose',
    MondayDayOpen: 'MondayDayOpen',
    MondayDayClose: 'MondayDayClose',
    TuesdayDayOpen: 'TuesdayDayOpen',
    TuesdayDayClose: 'TuesdayDayClose',
    WednesdayDayOpen: 'WednesdayDayOpen',
    WednesdayDayClose: 'WednesdayDayClose',
    ThursdayDayOpen: 'ThursdayDayOpen',
    ThursdayDayClose: 'ThursdayDayClose',
    FridayDayOpen: 'FridayDayOpen',
    FridayDayClose: 'FridayDayClose',
    SaturdayDayOpen: 'SaturdayDayOpen',
    SaturdayDayClose: 'SaturdayDayClose',
    Lat: 'LAT',
    Lng: 'LONG',
    GSCProviderID: 'PROVIDERID',
    GSCLicensedDate: 'FIELD18739',
    GSCEmail: 'FIELD19375'
  },

  url: 'https://v3.childcareresource.com/turbo/index.cfm?action=search.quicksearch',

  bindStreamEvents: function(stream, options) {
    var dataset = this;
    var parserStream = JSONStream.parse('*.DATA.*');
    var cleanerStream = new GSCJSONCleanerStream();
    var filter = _.bind(options.filter || this.filter, this);

    stream.pipe(cleanerStream)
    .pipe(parserStream)
    .on('data', function(data) {
      if (filter(data)) {
        dataset.emit('data', dataset.createRecord(data));
      }
    })
    .on('end', function(data) {
      dataset.emit('end');
    })
    .on('error', function(err) {
      console.log(err);
      dataset.emit('error', err);
    });

    return this;
  },

  getArchiveFilename: function(options) {
    options = options || {};
    var criteria = options.criteria;
    if (criteria && criteria.SEARCHTYPE) {
      if (criteria.SEARCHTYPE == 'Zip') {
        return criteria.ZIP + '.json'; 
      }
      else if (criteria.SEARCHTYPE == 'City') {
        return criteria.CITY + '.json';
      }
    }
    return 'quicksearch.json';
  },

  _makeRequest: function(url, options) {
    var filename = this.getArchiveFilename(options);
    var writer = fs.createWriteStream(this.getArchivePath(filename));
    var req = request.post({
      url: url, 
      body: {criteria: options.criteria},
      json: true
    });
    req.pipe(writer);
    this.bindStreamEvents(req, options);
  },

  fromURL: function(url, options) {
    var options = options || {};
    var url = url || this.url;
    var dataset = this;
    if (!gscAuthenticated) {
      gscAuthenticate(options.username, options.password, function(error, response, body) {
        dataset._makeRequest(url, options);
      });
    }
    else {
      this._makeRequest(url, options);
    }
    return this;
  },

  fromFile: function(fileName) {
    return this.bindStreamEvents(fs.createReadStream(fileName));
  }
});



var GSCProfileDataset = exports.GSCProfileDataset = function() {};
util.inherits(GSCProfileDataset, EarlyChildhoodDataset);

_.extend(GSCProfileDataset.prototype, {
  fields: [
    'LicenseNumber',
    'Transportation',
    'Meals',
    'GSCpts',
    'GSCptsStaff',
    'GSCptsFamily',
    'GSCptsAdmin',
    'GSCptsEnv',
    'GSCptsCurr',
    'GSCeligibility',
    'GSCsubsidy',
    'GSCspecial',
    'GSCcapacity',
    'GSCcontract',
    'GSCfee',
    'GSCcontact'
  ],

  crosswalk: {
    'LicenseNumber': 'LicenseID',
    'Address': 'Address',
    'City': 'City',
    'ZipCode': 'ZIP',
    'County': 'County',
    'Phone': 'Phone',
    'Website': 'Website',
    'AgesFrom': 'AcceptsAgesfrom',
    'AgesTo': '12 years, 11 months',
    'MonthsOfOperation': 'YearSchedule',
    'Transportation': 'ProvidesTransportation',
    'Environment': 'Environment',
    'Meals': 'MealsProvided',
    'GSCeligibility': 'ProgramEligibilityCriteria',
    'GSCpts': 'GSCpts',
    'GSCptsStaff': 'GSCptsStaff',
    'GSCptsFamily': 'GSCptsFamily',
    'GSCptsAdmin': 'GSCptsAdmin',
    'GSCptsEnv': 'GSCptsEnv',
    'GSCptsCurr': 'GSCptsCurr',
    'GSCsubsidy': 'FinancialAssistance',
    'GSCspecial': 'SpecialNeedsExperience',
    'GSCcapacity': 'TotalLicensedCapacity',
    'GSCcontract': 'Writtencontract',
    'GSCfee': 'ApplicationRegistrationFee',
    'GSCcontact': 'Contact'
  },

  url: 'https://v3.childcareresource.com/turbo/index.cfm?action=search.getProfile', 

  getArchiveFilename: function(options) {
    options = options || {};
    if (options.providerId) {
      return options.providerId + '.html';
    }

    return 'profile.html';
  },

  _makeRequest: function(url, options) {
    var dataset = this;
    var filename = this.getArchiveFilename(options);
    var writer = fs.createWriteStream(this.getArchivePath(filename));

    request.get({
      url: url
    }, function(error, response, body) {
      dataset.parse(body);
    })
    .pipe(writer);
  },

  parse: function(data) {
      var record = {};
      var $ = cheerio.load(data);


      var $ptsCells = $('*:contains("Points/Possible")').closest('table').find('tr').last().find('td');

      if ($ptsCells.length) {
        // Parse the points 
        // They're in the format Points/Possible
        // Total points
        record.GSCpts = $ptsCells.eq(0).text().split('/')[0];
        // Staff Qualifications and Professional Development
        record.GSCptsStaff = $ptsCells.eq(1).text().split('/')[0];
        // Family and Community Partnerships
        record.GSCptsFamily = $ptsCells.eq(2).text().split('/')[0];
        // Administration and Management
        record.GSCptsAdmin = $ptsCells.eq(3).text().split('/')[0];
        // Environment
        record.GSCptsEnv = $ptsCells.eq(4).text().split('/')[0];
        // Curriculum and Instruction
        record.GSCptsCurr = $ptsCells.eq(5).text().split('/')[0];
      }

      $('div.field').each(function(index, el) {
        var ws = /(^\s+|\s+$)/g;
        var labelText = $('.field-label', this).text(); 
        // Strip leading/trailing whitespace and ':' from the label
        var cleanLabel = labelText.replace(ws,'').replace(/:$/,'');
        // Convert label text to a key like ExampleFieldName
        var labelKey = cleanLabel.replace(/[\s?\/,]+/g, '');
        // The value of the field is all the text in this element, minus the
        // label text. Also strip any leading/trailing whitespace
        var value = $(this).text().replace(labelText,'').replace(ws, '');
        record[labelKey] = value;
      });
      this.emit('data', this.createRecord(record));
      this.emit('end');

      return this;
  },

  fromURL: function(url, options) {
    options = options || {};
    url = url || this.url;
    var fullUrl = url + '&providerId=' + options.providerId;
    var dataset = this;

    if (!gscAuthenticated) {
      gscAuthenticate(options.username, options.password, function(error, response, body) {
        dataset._makeRequest(fullUrl, options);
      });
    }
    else {
      this._makeRequest(fullUrl, options);
    }

    return this;
  },

  fromFile: function(fileName) {
    var dataset = this;
    fs.readFile(fileName, function (err, data) {
      if (err) throw err;
      dataset.parse(data);
    });
   
    return this;
  }
});

var GSCSpreadsheetDataset = exports.GSCSpreadsheetDataset = function() {};
util.inherits(GSCSpreadsheetDataset, EarlyChildhoodDataset);
_.extend(GSCSpreadsheetDataset.prototype, {
  crosswalk: {
    'LicenseNumber': 1,
    'County': 6
  },

  fromFile: function(fileName) {
    var dataset = this;
    csv().from.path(fileName)
    .transform(function(row, index, callback){
      var record = null;
      var county = row[dataset.crosswalk['County']];
      if (county == 'Wayne' ||
          county == 'Maycomb' ||
          county == 'Oakland') {
        record = {
          'LicenseNumber': row[dataset.crosswalk['LicenseNumber']]
        };
      }
      callback(null, record);
    })
    .on('record', function(record, index) {
      dataset.emit('data', record); 
    })
    .on('error', function(err) {
      console.log(err);
    })
    .on('end', function() {
      dataset.emit('end');
    });

    return this;
  }
});
