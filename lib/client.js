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
var xml2json = require('xml2json-edp');

exports = module.exports = Client;

exports.STR_MIN = 'STR_MIN';
exports.STR_MAX = 'STR_MAX';
exports.INT_MIN = 'INT_MIN';
exports.INT_MAX = 'INT_MAX';
exports.BOOL_MIN = 'BOOL_MIN';
exports.BOOL_MAX = 'BOOL_MAX';

/**
 * OTS Client.
 * @param {Object} options
 *  - {String} accessID
 *  - {String} accessKey
 *  - {Number} requestTimeout, default 5000ms.
 *  - {Agent} http request agent, default is `urllib.agent`.
 * @constructor
 */
function Client(options) {
  this.accessID = options.accessID;
  this.accessKey = options.accessKey;
  this.signatureMethod = options.signatureMethod || 'HmacSHA1';
  this.signatureVersion = options.signatureVersion || '1';
  this.APIVersion = options.APIVersion || '1';
  this.APIHost = options.APIHost || 'http://service.ots.aliyun.com';
  this.requestAgent = options.agent || null;
  this.requestTimeout = options.requestTimeout || 5000;
}

/**
 * Create a table group.
 * 
 * @param {String} name, group name.
 * @param {String} partitionKeyType, 'INTEGER' or 'STRING'.
 * @param {Function(err, result)} callback
 * @return {Client} this
 */
Client.prototype.createTableGroup = function (name, partitionKeyType, callback) {
  var params = [
    ['TableGroupName', name],
    ['PartitionKeyType', partitionKeyType]
  ];
  this.request('CreateTableGroup', params, function (err, result) {
    if (err) {
      return callback(err, result);
    }
    if (result.Code !== 'OK') {
      err = new Error('Create table group "' + name + '" error');
      err.name = 'CreateTableGroupError';
    }
    callback(err, result);
  });
  return this;
};

/**
 * List all table groups.
 * 
 * @param {Function(err, groupnames)} callback
 * @return {Client} this
 */
Client.prototype.listTableGroup = function (callback) {
  this.request('ListTableGroup', null, function (err, result) {
    if (err) {
      return callback(err, result);
    }
    var names = result.TableGroupNames.TableGroupName || [];
    if (!Array.isArray(names)) {
      names = [names];
    }
    callback(err, names);
  });
  return this;
};

/**
 * Delete a table group.
 * 
 * @param {String} name, group name.
 * @param {Function(err, result)} callback
 * @return {Client} this
 */
Client.prototype.deleteTableGroup = function (name, callback) {
  var params = [
    ['TableGroupName', name]
  ];
  this.request('DeleteTableGroup', params, function (err, result) {
    if (err) {
      return callback(err, result);
    }
    if (result.Code !== 'OK') {
      err = new Error('Delete table group "' + name + '" error');
      err.name = 'DeleteTableGroupError';
    }
    callback(err, result);
  });
  return this;
};

/**
 * List the table names.
 * @param {Function(err, tablenames)} callback
 * @return {Client} his
 */
Client.prototype.listTable = function (callback) {
  this.request('ListTable', null, function (err, result) {
    if (err) {
      return callback(err, result);
    }
    var names = result.TableNames.TableName || [];
    if (!Array.isArray(names)) {
      names = [names];
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
Client.prototype.createTable = function (meta, callback) {
  var err = null;
  meta = meta || {};
  var tablename = meta.TableName;
  var params = [
    ['TableName', tablename]
  ];
  var primaryKeys = meta.PrimaryKey || [];
  if (!Array.isArray(primaryKeys)) {
    primaryKeys = [primaryKeys];
  }
  for (var i = 0, l = primaryKeys.length; i < l; i++) {
    var index = i + 1;
    var primaryKey = primaryKeys[i];
    params.push(['PK.' + index + '.Name', primaryKey.Name]);
    params.push(['PK.' + index + '.Type', primaryKey.Type]);
  }
  if ('PagingKeyLen' in meta) {
    params.push(['PagingKeyLen', meta.PagingKeyLen]);
  }
  var views = meta.View || [];
  if (!Array.isArray(views)) {
    views = [views];
  }
  for (var i = 0, l = views.length; i < l; i++) {
    var view = views[i];
    var viewIndex = i + 1;
    var pre = 'View.' + viewIndex;
    params.push([ pre + '.Name', view.Name ]);
    var pks = view.PrimaryKey || [];
    if (!Array.isArray(pks)) {
      pks = [pks];
    }
    for (var j = 0, jl = pks.length; j < jl; j++) {
      var index = j + 1;
      var primaryKey = pks[j];
      params.push([pre + '.PK.' + index + '.Name', primaryKey.Name]);
      params.push([pre + '.PK.' + index + '.Type', primaryKey.Type]);
    }
    var columns = view.Column || [];
    if (!Array.isArray(columns)) {
      columns = [columns];
    }
    for (var j = 0, jl = columns.length; j < jl; j++) {
      var index = j + 1;
      var column = columns[j];
      params.push([pre + '.Column.' + index + '.Name', column.Name]);
      params.push([pre + '.Column.' + index + '.Type', column.Type]);
    }
  }
  if ('TableGroupName' in meta) {
    params.push(['TableGroupName', meta.TableGroupName]);
  }
  this.request('CreateTable', params, function (err, result, res) {
    if (err) {
      return callback(err, result);
    }
    if (result.Code !== 'OK') {
      err = new Error('Create "' + tablename + '" table error');
      err.name = 'CreateTableError';
    }
    callback(err, result);
  });
  return this;
};

/**
 * Get the table meta infomation.
 * @param  {String}   name     table name.
 * @param  {Function(err, meta)} callback
 * @return {Client}            this
 */
Client.prototype.getTableMeta = function (name, callback) {
  var params = [ 
    ['TableName', name],
  ];
  this.request('GetTableMeta', params, function (err, result) {
    if (err) {
      return callback(err, result);
    }
    callback(null, result.TableMeta);
  });
  return this; 
};

/**
 * Delete a table.
 * @param  {String} name, table name.
 * @param  {Function(err)} callback
 * @return {Client} this
 */
Client.prototype.deleteTable = function (name, callback) {
  var params = [ 
    ['TableName', name],
  ];
  this.request('DeleteTable', params, function (err, result, res) {
    if (err) {
      return callback(err, result);
    }
    if (result.Code !== 'OK') {
      err = new Error('Delete "' + name + '" table error');
      err.name = 'DeleteTableError';
    }
    callback(err, result);
  });
  return this;
};

/**
 * Start a transaction on a table or table group.
 * 
 * @param {String} entityName, table name or group name.
 * @param {String|Integer} partitionKey, partition key value. 
 * @param {Function(err, transactionID)} callback
 * @return {Client} this
 */
Client.prototype.startTransaction = function (entityName, partitionKey, callback) {
  partitionKey = this.convert({ Value: partitionKey });
  var params = [
    ['EntityName', entityName],
    ['PartitionKeyValue', partitionKey.Value],
    ['PartitionKeyType', partitionKey.Type],
  ];
  this.request('StartTransaction', params, function (err, result) {
    if (err) {
      return callback(err, result);
    }
    callback(null, result.TransactionID);
  });
  return this;
};

/**
 * Commit a transaction.
 * 
 * @param {String} transactionID
 * @param {Function(err)} callback
 * @return {Client} this
 */
Client.prototype.commitTransaction = function (transactionID, callback) {
  var params = [
    ['TransactionID', transactionID],
  ];
  this.request('CommitTransaction', params, function (err, result) {
    if (err) {
      return callback(err, result);
    }
    if (result.Code !== 'OK') {
      err = new Error('Commit "' + transactionID + '" transaction error');
      err.name = 'CommitTransactionError';
    }
    callback(err, result);
  });
  return this;
};

/**
 * Abort a transaction.
 * 
 * @param {String} transactionID
 * @param {Function(err)} callback
 * @return {Client} this
 */
Client.prototype.abortTransaction = function (transactionID, callback) {
  var params = [
    ['TransactionID', transactionID],
  ];
  this.request('AbortTransaction', params, function (err, result) {
    if (err) {
      return callback(err, result);
    }
    if (result.Code !== 'OK') {
      err = new Error('Abort "' + transactionID + '" transaction error');
      err.name = 'AbortTransactionError';
    }
    callback(err, result);
  });
  return this;
};

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
Client.prototype.putData = function (tableName, primaryKeys, columns, checking, transactionID, callback) {
  if (typeof checking === 'function') {
    callback = checking;
    checking = null;
    transactionID = null;
  } else if (typeof transactionID === 'function') {
    callback = transactionID;
    transactionID = null;
  }
  var params = [
    ['TableName', tableName]
  ];

  this.addParams(params, 'PK', primaryKeys);
  this.addParams(params, 'Column', columns);
  if (checking) {
    params.push(['Checking', checking]);
  }
  if (transactionID) {
    params.push(['TransactionID', transactionID]);
  }
  this.request('PutData', params, function (err, result) {
    if (err) { return callback(err, result); }
    if (result.Code !== 'OK') {
      err = new Error('PutData to "' + tableName + '" table error');
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
Client.prototype.getRow = function (tableName, primaryKeys, columnNames, transactionID, callback) {
  if (typeof columnNames === 'function') {
    callback = columnNames;
    columnNames = null;
    transactionID = null;
  } else if (typeof transactionID === 'function') {
    callback = transactionID;
    transactionID = null;
  }
  var params = [
    ['TableName', tableName]
  ];

  this.addParams(params, 'PK', primaryKeys);

  if (Array.isArray(columnNames)) {
    for (var i = 0, l = columnNames.length; i < l; i++) {
      params.push(['Column.' + (i + 1) + '.Name', columnNames[i]]);
    }
  }
  if (transactionID) {
    params.push(['TransactionID', transactionID]);
  }
  this.request('GetRow', params, function (err, result) {
    if (err) { return callback(err, result); }
    var row = null;
    var columns = result.Table.Row.Column;
    if (columns) {
      if (!Array.isArray(columns)) {
        columns = [columns];
      }
      row = {};
      for (var i = columns.length; i--;) {
        var col = columns[i];
        var parse = Client.ValueParsers[col.Value.type];
        // console.log(col.Value.type, parse)
        var val = col.Value.$t;
        row[col.Name] = parse ? parse(val) : val;
      }
    }
    callback(err, row);
  });
};

/**
 * Get multi rows from a table or view by primary key range.
 * 
 * @param {String} tableName, the table name.
 * @param {Array|Object|null} primaryKeys, list of primary keys.
 *  - {Object} primaryKey: {
 *    Name: 'id', Value: 1000
 *  }
 * @param {Array|Object} primaryKeyRanges, list of primary key range.
 *  - {Object} primaryKeyRange: 
 *    { Name: 'id', Begin: 1000, End: 1010 }
 *    { Name: 'id', Begin: exports.INT_MIN, End: exports.INT_MAX }
 *    { Name: 'name', Begin: exports.STR_MIN, End: exports.STR_MAX }
 *    { Name: 'enable', Begin: exports.BOOL_MIN, End: exports.BOOL_MAX }
 * @param {Array|null} columnNames, `null` return all columns.
 * @param {Integer} [top], return top rows, set `null` to return all match rows.
 * @param {String} [transactionID], transaction id which return by startTransaction().
 * @param {Function(err, row)} callback 
 * @return {Client} this
 */
Client.prototype.getRowsByRange = 
function (tableName, primaryKeys, primaryKeyRanges, columnNames, top, transactionID, callback) {
  if (typeof top === 'function') {
    callback = top;
    top = null;
    transactionID = null;
  } else if (typeof transactionID === 'function') {
    callback = transactionID;
    transactionID = null;
  }
  var params = [
    ['TableName', tableName]
  ];
  var indexStart = 1;
  if (primaryKeys) {
    this.addParams(params, 'PK', primaryKeys);
    indexStart += primaryKeys.length || 1;
  }
  if (!Array.isArray(primaryKeyRanges)) {
    primaryKeyRanges = [ primaryKeyRanges ];
  }
  for (var i = 0, l = primaryKeyRanges.length; i < l; i++) {
    var range = primaryKeyRanges[i];
    var begin = this.convert({ Value: range.Begin });
    var end = this.convert({ Value: range.End });
    var pre = 'PK.' + (i + indexStart) + '.';
    params.push([pre + 'Name', range.Name]);
    params.push([pre + 'RangeBegin', begin.Value]);
    params.push([pre + 'RangeEnd', end.Value]);
    params.push([pre + 'RangeType', end.Type]);
  }
  if (Array.isArray(columnNames)) {
    for (var i = 0, l = columnNames.length; i < l; i++) {
      params.push(['Column.' + (i + 1) + '.Name', columnNames[i]]);
    }
  }
  if (top) {
    params.push(['Top', top]);
  }
  if (transactionID) {
    params.push(['TransactionID', transactionID]);
  }
  var cb = function (err, result) {
    if (err) {
      return callback(err, result);
    }
    callback(err, this.parseRows(result));
  }.bind(this);
  this.request('GetRowsByRange', params, cb);
};

/**
 * Get multi rows from a table or view by paging offset.
 * 
 * @param {String} tableName, the table name.
 * @param {Array|Object} pagingKeys, list of paging keys.
 *  - {Object} pagingKey: {
 *    Name: 'uid', Value: 'mk2'
 *  }
 * @param {Array|null} columnNames, `null` return all columns.
 * @param {Integer} offset, read start offset.
 * @param {Integer} top, return top rows.
 * @param {String} [transactionID], transaction id which return by startTransaction().
 * @param {Function(err, row)} callback 
 * @return {Client} this
 */
Client.prototype.getRowsByOffset = 
function (tableName, pagingKeys, columnNames, offset, top, transactionID, callback) {
  if (typeof transactionID === 'function') {
    callback = transactionID;
    transactionID = null;
  }
  var params = [
    ['TableName', tableName],
  ];
  this.addParams(params, 'Paging', pagingKeys);
  if (Array.isArray(columnNames)) {
    for (var i = 0, l = columnNames.length; i < l; i++) {
      params.push(['Column.' + (i + 1) + '.Name', columnNames[i]]);
    }
  }
  params.push(['Offset', offset]);
  params.push(['Top', top]);
  if (transactionID) {
    params.push(['TransactionID', transactionID]);
  }
  this.request('GetRowsByOffset', params, (function (err, result) {
    if (err) {
      return callback(err, result);
    }
    callback(err, this.parseRows(result));
  }).bind(this));
};

Client.prototype.parseRows = function (result) {
  var rows = result.Table.Row || [];
  if (!Array.isArray(rows)) {
    rows = [rows];
  }
  var need = [];
  for (var j = 0, jl = rows.length; j < jl; j++) {
    var columns = rows[j].Column;
    if (columns) {
      if (!Array.isArray(columns)) {
        columns = [columns];
      }
      var row = {};
      for (var i = columns.length; i--; ) {
        var col = columns[i];
        var parse = Client.ValueParsers[col.Value.type];
        // console.log(col.Value.type, parse)
        var val = col.Value.$t;
        row[col.Name] = parse ? parse(val) : val;
      }
      need.push(row);
    }
  }
  return need;
};

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
Client.prototype.deleteData = function (tableName, primaryKeys, columnNames, transactionID, callback) {
  if (typeof columnNames === 'function') {
    callback = columnNames;
    columnNames = null;
    transactionID = null;
  } else if (typeof transactionID === 'function') {
    callback = transactionID;
    transactionID = null;
  }
  var params = [
    ['TableName', tableName]
  ];

  this.addParams(params, 'PK', primaryKeys);

  if (Array.isArray(columnNames)) {
    for (var i = 0, l = columnNames.length; i < l; i++) {
      params.push([ 'Column.' + (i + 1) + '.Name', columnNames[i] ]);
    }
  }
  if (transactionID) {
    params.push(['TransactionID', transactionID]);
  }
  this.request('DeleteData', params, function (err, result) {
    if (err) {
      return callback(err, result);
    }
    if (result.Code !== 'OK') {
      err = new Error('DeleteData from "' + tableName + '" table error');
      err.name = 'DeleteDataError';
    }
    callback(err, result);
  });
};

/**
 * Batch modify data on a transaction.
 * 
 * @param {String} tableName
 * @param {Array} modifyItems, each modifyItem is a complex object, an example:
 *   { 
 *     Type: 'PUT', 
 *     PrimaryKeys: [ { Name: 'uid', Value: variant-value }, { Name: 'mailid', Value: variant-value } ],
 *     Columns: [ { Name: 'email', Value: 'fengmk@gmail.com' } ]
 *     Checking: 'NO'
 *    }
 *  - {String} Type, could be PUT or DELETE; 
 *  - {Array|Object} PrimaryKeys
 *  - {Array|Object} [Columns], optional.
 *  - {String} Checking, could be 'INSERT', 'UPDATE', 'NO', default is 'NO'; 
 *    when `Type` is 'DELETE', `Checking` would be ignored.
 * @param {String} transactionID, must provide the transcation id.
 * @param {Function(err, result)} callback
 */
Client.prototype.batchModifyData = function (tableName, modifyItems, transactionID, callback) {
  var params = [
    ['TableName', tableName],
  ];
  for (var i = 0, l = modifyItems.length; i < l; i++) {
    var item = modifyItems[i];
    var pre = 'Modify.' + (i + 1) + '.';
    params.push([pre + 'Type', item.Type]);
    this.addParams(params, pre + 'PK', item.PrimaryKeys);
    var columns = item.Columns;
    if (columns) {
      var columnPre = pre + 'Column';
      if (item.Type === 'PUT') {
        this.addParams(params, columnPre, columns);
      } else {
        if (!Array.isArray(columns)) {
          columns = [columns];
        }
        for (var j = 0, jl = columns.length; j < jl; j++) {
          var p = columnPre + '.' + (j + 1) + '.';
          var col = columns[j];
          params.push([p + 'Name', col.Name]);
        }
      }
    }
    if (item.Checking) {
      params.push([pre + 'Checking', item.Checking]);
    }
  }
  params.push(['TransactionID', transactionID]);
  this.request('BatchModifyData', params, function (err, result) {
    if (err) {
      return callback(err, result);
    }
    if (result.Code !== 'OK') {
      err = new Error('Batch modify data on transaction "' + transactionID + '" error.');
      err.name = 'BatchModifyDataError';
    }
    callback(err, result);
  });
};

Client.prototype.addParams = function (params, pre, items) {
  if (!Array.isArray(items)) {
    items = [items];
  }
  for (var i = 0, l = items.length; i < l; i++) {
    var keypre = pre + '.' + (i + 1) + '.';
    var item = items[i];
    item = this.convert(item);
    params.push([keypre + 'Name', item.Name]);
    params.push([keypre + 'Value', item.Value]);
    params.push([keypre + 'Type', item.Type]);
  }
};

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
Client.prototype.convert = function (o) {
  var value = o.Value;
  var type = 'STRING';
  switch (value) {
    case exports.STR_MIN:
      value = 'INF_MIN';
      type = 'STRING';
      break;
    case exports.STR_MAX:
      value = 'INF_MAX';
      type = 'STRING';
      break;
    case exports.INT_MIN:
      value = 'INF_MIN';
      type = 'INTEGER';
      break;
    case exports.INT_MAX:
      value = 'INF_MAX';
      type = 'INTEGER';
      break;
    case exports.BOOL_MIN:
      value = 'INF_MIN';
      type = 'BOOLEAN';
      break;
    case exports.BOOL_MAX:
      value = 'INF_MAX';
      type = 'BOOLEAN';
      break;
    default:
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
      break;
  }
  o.Value = value;
  o.Type = type;
  return o;
};

Client.ValueParsers = {
  DOUBLE: parseFloat,
  INTEGER: parseInt,
  BOOLEAN: function (o) {
    return o === 'TRUE';
  }
};

/**
 * Signature the params.
 * 
 * Base64(hmac-sha1(VERB + “\n” + “KEY1=VALUE1” + “&” + “KEY2=VALUE2”, AccessKey))
 *   
 * @param  {Array} params
 * @return {String} signature base64 string.
 */
Client.prototype.signature = function (url, params) {
  params = params || [];
  params.push(['APIVersion', this.APIVersion]);
  params.push(['Date', new Date().toGMTString()]);
  params.push(['OTSAccessKeyId', this.accessID]);
  params.push(['SignatureMethod', this.signatureMethod]);
  params.push(['SignatureVersion', this.signatureVersion]);
  
  var sorted = [];
  for (var i = 0, l = params.length; i < l; i++) {
    var param = params[i];
    param = param[0] + '=' + encodeURIComponent(param[1]);
    params[i] = param;
    sorted.push(param);
  }
  sorted.sort();
  var basestring = url + '\n' + sorted.join('&');
  var sha1 = crypto.createHmac('sha1', this.accessKey);
  sha1.update(basestring);
  var sign = sha1.digest('base64');
  params.push('Signature=' + encodeURIComponent(sign));
  return params;
};

/**
 * Request the OTS
 * @param {String} optype, operate type, eg: 'CreateTable', 'DeleteTable'
 * @param {Object} params
 * @param {Function(err, result)} callback
 * @return {Client} client instance.
 */
Client.prototype.request = function (optype, params, callback) {
  var url = '/' + optype;
  params = this.signature(url, params);
  url = this.APIHost + url;
  var body = params.join('&');
  urllib.request(url, {
    content: body,
    type: 'POST',
    timeout: this.requestTimeout,
    agent: this.requestAgent
  }, function (err, result, res) {
    if (err) {
      err.name = 'OTS' + err.name;
      err.url = url + '?' + body;
      return callback(err);
    }
    if (result) {
      result = xml2json.toJson(result, {object: true, space: true, sanitize: false});
    }
    if (result.Error) {
      err = new Error(result.Error.Message);
      err.name = result.Error.Code || 'OTSError';
      err.url = url + '?' + body;
      err.data = result;
    } else {
      var key = optype + 'Result';
      result = result[key] || result;
    }
    callback(err, result, res);
  });
};

/**
 * Create a client.
 * @param  {Object} options
 * @return {Client}
 */
module.exports.createClient = function (options) {
  return new Client(options);
};