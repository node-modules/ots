/*!
 * ots - lib/client.js
 * Copyright(c) 2012 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

/**
 * Module dependencies.
 */

var urllib = require('urllib');
var crypto = require('crypto');
var xml2json = require('xml2json');


exports = module.exports = Client;

/**
 * OTS Client.
 * @param {Object} options
 *  - {String} AccessID
 *  - {String} AccessKey
 *  - {Number} requestTimeout, default 5000ms.
 * @constructor
 */
function Client(options) {
  this.AccessID = options.AccessID;
  this.AccessKey = options.AccessKey;
  this.SignatureMethod = options.SignatureMethod || 'HmacSHA1';
  this.SignatureVersion = options.SignatureVersion || '1';
  this.APIVersion = options.APIVersion || '1';
  this.APIHost = options.APIHost || 'http://service.ots.aliyun.com';
  this.requestTimeout = options.requestTimeout || 5000;
}

/**
 * List the table names.
 * @param {Function(err, tablenames)} callback
 * @return {Client} his
 */
Client.prototype.listTable = function(callback) {
  this.request('ListTable', null, 'GET', function(err, result) {
    if (err) return callback(err, result);
    var names = result.TableNames.TableName || [];
    if (!Array.isArray(names)) {
      names = [ names ];
    }
    callback(null, names);
  });
};

/**
 * Create a table.
 *
 * @param {Object} meta, table meta info.
 * @param {Function(err, result)} callback
 * 
 * TableMeta class includes all information of a table infomation and its views; 
 * if it belongs to a TableGroup, we will specify the Table Group Name.
 * A table meta example is:

  tableMeta: {
    'TableName' : 'ExampleTableName',
    'PrimaryKey' : [
      {'Name':'uid', 'Type':'STRING'},
      {'Name':'flag', 'Type':'STRING'},
      {'Name':'updatetime', 'Type':'STRING'},
      {'Name':'docid', 'Type':'STRING'} 
    ],
    'PagingKeyLen' : 2,
    // A view list example is:
    View: [
      { 
        'Name': 'view1', 
        'PrimaryKey' : [
          {'Name':'uid', 'Type':'STRING'},
          {'Name':'flag', 'Type':'STRING'},
          {'Name':'updatetime', 'Type':'STRING'},
          {'Name':'docid', 'Type':'STRING'},
        ],
        'Column' : [
          {'Name':'uid', 'Type':'STRING'},
          {'Name':'flag', 'Type':'STRING'},
          {'Name':'updatetime', 'Type':'STRING'},
          {'Name':'docid', 'Type':'STRING'},
        ],
        'PagingKeyLen' : 2
      }, ...
    ],
    TableGroupName: name or undefined
  }
 */ 
Client.prototype.createTable = function(meta, callback) {
  var err = null;
  var meta = meta || {};
  var tablename = meta.TableName;
  var params = [
    [ 'TableName', tablename ],
  ];
  var primaryKeys = meta.PrimaryKey || [];
  if (!Array.isArray(primaryKeys)) {
    primaryKeys = [ primaryKeys ];
  }
  for (var i = 0, l = primaryKeys.length; i < l; i++) {
    var index = i + 1;
    var primaryKey = primaryKeys[i];
    params.push([ 'PK.' + index + '.Name', primaryKey.Name ]);
    params.push([ 'PK.' + index + '.Type', primaryKey.Type ]);
  }
  if ('PagingKeyLen' in meta) {
    params.push([ 'PagingKeyLen', meta.PagingKeyLen ]);
  }
  var views = meta.View || [];
  if (!Array.isArray(views)) {
    views = [ views ];
  }
  for (var i = 0, l = views.length; i < l; i++) {
    var view = views[i];
    var viewIndex = i + 1;
    var pre = 'View.' + viewIndex;
    params.push([ pre + '.Name', view.Name ]);
    var pks = view.PrimaryKey || [];
    if (!Array.isArray(pks)) {
      pks = [ pks ];
    }
    for (var j = 0, jl = pks.length; j < jl; j++) {
      var index = j + 1;
      var primaryKey = pks[j];
      params.push([ pre + '.PK.' + index + '.Name', primaryKey.Name ]);
      params.push([ pre + '.PK.' + index + '.Type', primaryKey.Type ]);
    }
    var columns = view.Column || [];
    if (!Array.isArray(columns)) {
      columns = [ columns ];
    }
    for (var j = 0, jl = columns.length; j < jl; j++) {
      var index = j + 1;
      var column = columns[j];
      params.push([ pre + '.Column.' + index + '.Name', column.Name ]);
      params.push([ pre + '.Column.' + index + '.Type', column.Type ]);
    }
  }
  if ('TableGroupName' in meta) {
    params.push([ 'TableGroupName', meta.TableGroupName ]);
  }
  this.request('CreateTable', params, 'POST', function(err, result, res) {
    if (err) return callback(err, result);
    if (result.Code !== 'OK') {
      err = new Error('Create "' + tablename + '" table error');
      err.name = 'CreateTableError';
    }
    callback(err, result);
  });
  return this;
}

/**
 * Get the table meta infomation.
 * @param  {String}   name     table name.
 * @param  {Function(err, meta)} callback
 * @return {Client}            this
 */
Client.prototype.getTableMeta = function(name, callback) {
  var params = [ 
    [ 'TableName', name ],
  ];
  this.request('GetTableMeta', params, 'GET', function(err, result) {
    if (err) return callback(err, result);
    callback(null, result.TableMeta);
  });
  return this; 
}

/**
 * Delete a table.
 * @param  {String} name, table name.
 * @param  {Function(err)} callback
 * @return {Client} this
 */
Client.prototype.deleteTable = function(name, callback) {
  var params = [ 
    [ 'TableName', name ],
  ];
  this.request('DeleteTable', params, 'POST', function(err, result, res) {
    if (err) return callback(err, result);
    if (result.Code !== 'OK') {
      err = new Error('Delete "' + name + '" table error');
      err.name = 'DeleteTableError';
    }
    callback(err, result);
  });
  return this;
}

/**
 * Insert or update a data row.
 * 
 * @param  {String} tableName     
 * @param  {Array|Object} primaryKeys   
 * @param  {Array} columns       
 * @param  {String} checking, 'NO', 'INSERT', 'UPDATE', default is 'NO'.
 * @param  {String} transactionID
 * @param  {Function(err, result)} callback
 * @return {Client} this
 */
Client.prototype.putData = function(tableName, primaryKeys, columns, checking, transactionID, callback) {
  if (typeof(checking) === 'function') {
    callback = checking;
    checking = null;
    transactionID = null;
  } else if (typeof(transactionID) === 'function') {
    callback = transactionID;
    transactionID = null;
  }
  var params = [
    [ 'TableName', tableName ]
  ];

  this.addParams(params, 'PK', primaryKeys);
  this.addParams(params, 'Column', columns);
  if (checking) {
    params.push([ 'Checking', checking ]);
  }
  if (transactionID) {
    params.push( [ 'TransactionID', TransactionID ]);
  }
  this.request('PutData', params, 'POST', function(err, result) {
    if (err) return callback(err, result);
    if (result.Code !== 'OK') {
      err = new Error('PutData to "' + name + '" table error');
      err.name = 'PutDataError';
    }
    callback(err, result);
  });
};

/**
 * Get a row data from a table or view.
 * 
 * @param {String} tableName
 * @param {Array|Object} primaryKeys, list of primary keys.
 *  - {Object} primaryKey: {
 *    Name: 'uid', Value: 'mk2'
 *  }
 * @param {Array} [columnNames], default is `null`, return all columns.
 * @param {String} [transactionID], transaction id which return by StartTransactionAPI().
 * @param {Function(err, row)} callback 
 * @return {Client} this
 */
Client.prototype.getRow = function(tableName, primaryKeys, columnNames, transactionID, callback) {
  if (typeof(columnNames) === 'function') {
    callback = columnNames;
    columnNames = null;
    transactionID = null;
  } else if (typeof(transactionID) === 'function') {
    callback = transactionID;
    transactionID = null;
  }
  var params = [
    [ 'TableName', tableName ]
  ];

  this.addParams(params, 'PK', primaryKeys);

  if (Array.isArray(columnNames)) {
    for (var i = 0, l = columnNames.length; i < l; i++) {
      params.push([ 'Column.' + (i + 1) + '.Name', columnNames[i] ]);
    }
  }
  if (transactionID) {
    params.push([ 'TransactionID', transactionID ]);
  }
  this.request('GetRow', params, 'GET', function(err, result) {
    if (err) return callback(err, result);
    var row = null;
    var columns = result.Table.Row.Column;
    if (columns) {
      if (!Array.isArray(columns)) {
        columns = [ columns ];
      }
      row = {};
      for (var i = columns.length; i--; ) {
        var col = columns[i];
        var parse = Client.ValueParsers[col.Value.type];
        // console.log(col.Value.type, parse)
        var val = col.Value['$t'];
        row[col.Name] = parse ? parse(val) : val;
      }
    }
    callback(err, row);
  });
}

/**
 * Delete a data row from a table.
 * 
 * @param {String} tableName
 * @param {Array|Object} primaryKeys, list of primary keys.
 *  - {Object} primaryKey: {
 *    Name: 'uid', Value: 'mk2'
 *  }
 * @param {Array} [columnNames], default is `null`, delete a row. Otherise, delete the columns.
 * @param {String} [transactionID], transaction id which return by StartTransactionAPI().
 * @param {Function(err, row)} callback 
 * @return {Client} this
 */
Client.prototype.deleteData = function(tableName, primaryKeys, columnNames, transactionID, callback) {
  if (typeof(columnNames) === 'function') {
    callback = columnNames;
    columnNames = null;
    transactionID = null;
  } else if (typeof(transactionID) === 'function') {
    callback = transactionID;
    transactionID = null;
  }
  var params = [
    [ 'TableName', tableName ]
  ];

  this.addParams(params, 'PK', primaryKeys);

  if (Array.isArray(columnNames)) {
    for (var i = 0, l = columnNames.length; i < l; i++) {
      params.push([ 'Column.' + (i + 1) + '.Name', columnNames[i] ]);
    }
  }
  if (transactionID) {
    params.push([ 'TransactionID', transactionID ]);
  }
  this.request('DeleteData', params, 'POST', function(err, result) {
    if (err) return callback(err, result);
    if (result.Code !== 'OK') {
      err = new Error('DeleteData from "' + name + '" table error');
      err.name = 'DeleteDataError';
    }
    callback(err, result);
  });
}

Client.prototype.addParams = function(params, pre, items) {
  if (!Array.isArray(items)) {
    items = [ items ];
  }
  for (var i = 0, l = items.length; i < l; i++) {
    var keypre = pre + '.' + (i + 1) + '.';
    var item = items[i];
    item = this.convert(item);
    params.push([ keypre + 'Name', item.Name ]);
    params.push([ keypre + 'Value', item.Value ]);
    params.push([ keypre + 'Type', item.Type ]);
  }
}

/**
 * Convert javascript data to OTS data. 
 * 
 * { Name: 'uid', Value: 'foo' } => { Name: 'uid', Value: "'foo'", Type: 'STRING' }
 * { Name: 'id', Value: 123 } => { Name: 'id', Value: 123, Type: 'INTEGER' }
 * { Name: 'price', Value: 100.85 } => { Name: 'price', Value: 100.85, Type: 'DOUBLE' }
 * { Name: 'enable', Value: true } => { Name: 'enable', Value: true, Type: 'BOOLEAN' }
 * 
 * @param  {Object} o
 *  - {String} Value
 * @return {Object} the OTS data contain `Type`. 
 */
Client.prototype.convert = function(o) {
  var value = o.Value;
  var type = 'STRING';
  var t = typeof(value);
  if (t === 'string') {
    value = "'" + value + "'";
  } else if (t === 'boolean') {
    type = 'BOOLEAN';
  } else if (t === 'number') {
    type = value % 1 === 0 ? 'INTEGER' : 'DOUBLE';
  } else {
    value = "'" + String(value) + "'";
  }
  o.Value = value;
  o.Type = type;
  return o;
}

Client.ValueParsers = {
  'DOUBLE': parseFloat,
  'INTEGER': parseInt,
  'BOOLEAN': function(o) { return o === 'TRUE'; },
};

/**
 * Signature the params.
 * 
 * Base64(hmac-sha1(VERB + “\n” + “KEY1=VALUE1” + “&” + “KEY2=VALUE2”, AccessKey))
 *   
 * @param  {Array} params
 * @return {String} signature base64 string.
 */
Client.prototype.signature = function(url, params) {
  params = params || [];
  params.push([ 'APIVersion', this.APIVersion ]);
  params.push([ 'Date', new Date().toGMTString() ]);
  params.push([ 'OTSAccessKeyId', this.AccessID ]);
  params.push([ 'SignatureMethod', this.SignatureMethod ]);
  params.push([ 'SignatureVersion', this.SignatureVersion ]);
  
  var sorted = [];
  for (var i = 0, l = params.length; i < l; i++) {
    var param = params[i];
    param = param[0] + '=' + encodeURIComponent(param[1]);
    params[i] = param;
    sorted.push(param);
  }
  sorted.sort();
  var basestring = url + '\n' + sorted.join('&');
  var sha1 = crypto.createHmac('sha1', this.AccessKey);
  sha1.update(basestring);
  var sign = sha1.digest('base64');
  params.push('Signature=' + encodeURIComponent(sign));
  return params;
};

/**
 * Request the OTS
 * @param {String} optype, operate type, eg: 'CreateTable', 'DeleteTable'
 * @param {Object} params
 * @param {String} method, 'GET', 'POST'
 * @param {Function(err, result)} callback
 * @return {Client} client instance.
 */
Client.prototype.request = function(optype, params, method, callback) {
  var url = '/' + optype;
  params = this.signature(url, params);
  url = this.APIHost + url;
  var body = params.join('&');
  urllib.request(url, {
    content: body,
    type: method,
    timeout: this.requestTimeout,
  }, function(err, result, res) {
    if (err) {
      err.url = url;
      err.params = body;
      return callback(err);
    }
    if (result) {
      result = xml2json.toJson(result, { object: true });
    }
    if (result.Error) {
      err = new Error(result.Error.Message);
      err.name = result.Error.Code || 'OSTError';
    } else {
      var key = optype + 'Result';
      result = result[key] || result;
    }
    if (err) {
      err.url = url;
      err.params = body;
    }
    callback(err, result, res);
  });
}
