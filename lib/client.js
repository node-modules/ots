/*!
 * ots - lib/client.js
 * Copyright(c) 2012 - 2013 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

/**
 * Module dependencies.
 */

var fs = require('fs');
var path = require('path');
var urlparse = require('url').parse;
var urllib = require('urllib');
var utility = require('utility');
var CacheDNS = require('cache-dns');
var debug = require('debug')('ots');
var Schema = require('protobuf').Schema;

exports = module.exports = Client;

exports.STR_MIN = 'STR_MIN';
exports.STR_MAX = 'STR_MAX';
exports.INT_MIN = 'INT_MIN';
exports.INT_MAX = 'INT_MAX';
exports.BOOL_MIN = 'BOOL_MIN';
exports.BOOL_MAX = 'BOOL_MAX';

// "schema" contains all message types defined in buftest.proto|desc.
var schema = new Schema(fs.readFileSync(path.join(path.dirname(__dirname), 'ots_protocol.desc')));

var ColumnType = module.exports.ColumnType = {
  INF_MIN: 0,
  INF_MAX: 1,
  INTEGER: 2,
  STRING: 3,
  BOOLEAN: 4,
  DOUBLE: 5,
};

var ErrorMessage = schema['com.aliyun.cloudservice.ots.ErrorMessage'];
// The "CreateTableGroupRequest" message.
var CreateTableGroupRequest = schema['com.aliyun.cloudservice.ots.CreateTableGroupRequest'];
var DeleteTableGroupRequest = schema['com.aliyun.cloudservice.ots.DeleteTableGroupRequest'];
var ListTableGroupResponse = schema['com.aliyun.cloudservice.ots.ListTableGroupResponse'];

var CreateTableRequest = schema['com.aliyun.cloudservice.ots.CreateTableRequest'];
var DeleteTableRequest = schema['com.aliyun.cloudservice.ots.DeleteTableRequest'];
var GetTableMetaRequest = schema['com.aliyun.cloudservice.ots.DeleteTableRequest'];
var GetTableMetaResponse = schema['com.aliyun.cloudservice.ots.GetTableMetaResponse'];
var ListTableResponse = schema['com.aliyun.cloudservice.ots.ListTableResponse'];

var StartTransactionRequest = schema['com.aliyun.cloudservice.ots.StartTransactionRequest'];
var StartTransactionResponse = schema['com.aliyun.cloudservice.ots.StartTransactionResponse'];
var AbortTransactionRequest = schema['com.aliyun.cloudservice.ots.AbortTransactionRequest'];
var CommitTransactionRequest = schema['com.aliyun.cloudservice.ots.CommitTransactionRequest'];

var PutRowRequest = schema['com.aliyun.cloudservice.ots.PutRowRequest'];
var GetRowRequest = schema['com.aliyun.cloudservice.ots.GetRowRequest'];
var GetRowResponse = schema['com.aliyun.cloudservice.ots.GetRowResponse'];
var DeleteRowRequest = schema['com.aliyun.cloudservice.ots.DeleteRowRequest'];
var MultiGetRowRequest = schema['com.aliyun.cloudservice.ots.MultiGetRowRequest'];
var MultiGetRowResponse = schema['com.aliyun.cloudservice.ots.MultiGetRowResponse'];
var GetRowsByRangeRequest = schema['com.aliyun.cloudservice.ots.GetRowsByRangeRequest'];
var GetRowsByRangeResponse = schema['com.aliyun.cloudservice.ots.GetRowsByRangeResponse'];
var MultiPutRowRequest = schema['com.aliyun.cloudservice.ots.MultiPutRowRequest'];
var MultiPutRowResponse = schema['com.aliyun.cloudservice.ots.MultiPutRowResponse'];
var MultiDeleteRowRequest = schema['com.aliyun.cloudservice.ots.MultiDeleteRowRequest'];
var MultiDeleteRowResponse = schema['com.aliyun.cloudservice.ots.MultiDeleteRowResponse'];

var ResponseMap = {
  ListTableGroup: ListTableGroupResponse,
  GetTableMeta: GetTableMetaResponse,
  ListTable: ListTableResponse,
  StartTransaction: StartTransactionResponse,
  GetRow: GetRowResponse,
  MultiGetRow: MultiGetRowResponse,
  GetRowsByRange: GetRowsByRangeResponse,
  MultiPutRow: MultiPutRowResponse,
  MultiDeleteRow: MultiDeleteRowResponse,
};

/**
 * OTS Client.
 * 
 * @param {Object} options
 *  - {String} accessID
 *  - {String} accessKey
 *  - {Number} [requestTimeout], default 5000ms.
 *  - {Agent} [http] request agent, default is `urllib.agent`.
 *  - {Number} [dnsCacheTime] dns cache time, default is `10000 ms`.
 *  - {String} [APIVersion] api version, default is '2013-05-10'.
 *  - {String} [APIHost] api host URL, default is 'http://service.ots.aliyun.com'.
 * @constructor
 */
function Client(options) {
  this.accessID = options.accessID;
  this.accessKey = options.accessKey;
  this.signatureMethod = options.signatureMethod || 'HmacSHA1';
  this.signatureVersion = options.signatureVersion || '1';
  this.APIVersion = options.APIVersion || '2013-05-10';
  this.APIHost = options.APIHost || 'http://service.ots.aliyun.com';
  // protocol: 'http:'
  // hostname: 'service.ots.aliyun.com'
  // port: undefined
  this.APIHostInfo = urlparse(this.APIHost);
  this.requestAgent = options.agent || null;
  this.requestTimeout = options.requestTimeout || 5000;
  var dnsCacheTime = options.dnsCacheTime || 10000;
  this.dns = CacheDNS.create({cacheTime: dnsCacheTime});
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
  var params = {
    tableGroupName: name,
    partitionKeyType: ColumnType[partitionKeyType],
  };
  var body = CreateTableGroupRequest.serialize(params);
  this.request('CreateTableGroup', body, callback);
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
    var names = result.tableGroupNames || [];
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
  var params = {tableGroupName: name};
  var body = DeleteTableGroupRequest.serialize(params);
  this.request('DeleteTableGroup', body, callback);
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
    var names = result.tableNames || [];
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

  var tableMeta = {
    tableName: tablename,
    views: [],
    primaryKeys: []
  };
  if (meta.TableGroupName) {
    tableMeta.tableGroupName = meta.TableGroupName;
  }

  var primaryKeys = meta.PrimaryKey || [];
  if (!Array.isArray(primaryKeys)) {
    primaryKeys = [primaryKeys];
  }
  for (var i = 0, l = primaryKeys.length; i < l; i++) {
    var primaryKey = primaryKeys[i];
    tableMeta.primaryKeys.push({name: primaryKey.Name, type: primaryKey.Type});
  }

  var views = meta.View || [];
  if (!Array.isArray(views)) {
    views = [views];
  }

  for (var i = 0, l = views.length; i < l; i++) {
    var view = views[i];
    var viewIndex = i + 1;
    var pre = 'View.' + viewIndex;
    
    var pks = view.PrimaryKey || [];
    if (!Array.isArray(pks)) {
      pks = [pks];
    }

    var primaryKeys = [];
    for (var j = 0, jl = pks.length; j < jl; j++) {
      var pk = pks[j];
      primaryKeys.push({
        name: pk.Name,
        type: pk.Type
      });
    }
    var columns = view.Column || [];
    if (!Array.isArray(columns)) {
      columns = [columns];
    }
    var cols = [];
    for (var j = 0, jl = columns.length; j < jl; j++) {
      var column = columns[j];
      cols.push({
        name: column.Name,
        type: column.Type
      });
    }
    tableMeta.views.push({
      viewName: view.Name,
      primaryKeys: primaryKeys,
      columns: cols
    });
  }

  var body = CreateTableRequest.serialize({
    tableMeta: tableMeta
  });
  // console.log(tableMeta)
  // console.log(body, CreateTableRequest.parse(body));
  this.request('CreateTable', body, callback);
  return this;
};

/**
 * Get the table meta infomation.
 * @param  {String}   name     table name.
 * @param  {Function(err, meta)} callback
 * @return {Client}            this
 */
Client.prototype.getTableMeta = function (name, callback) {
  var body = GetTableMetaRequest.serialize({tableName: name});
  this.request('GetTableMeta', body, function (err, result) {
    if (err) {
      return callback(err, result);
    }
    callback(null, result.tableMeta);
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
  var body = DeleteTableRequest.serialize({tableName: name});
  this.request('DeleteTable', body, callback);
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
  var body = StartTransactionRequest.serialize({
    entityName: entityName,
    partitionKeyValue: this.convertToColumnValue({ Value: partitionKey })
  });
  this.request('StartTransaction', body, function (err, result) {
    if (err) {
      return callback(err, result);
    }
    callback(null, result.transactionId);
  });
  return this;
};

/**
 * Commit a transaction.
 * 
 * @param {String} transactionId
 * @param {Function(err)} callback
 * @return {Client} this
 */
Client.prototype.commitTransaction = function (transactionId, callback) {
  var body = CommitTransactionRequest.serialize({transactionId: transactionId});
  this.request('CommitTransaction', body, callback);
  return this;
};

/**
 * Abort a transaction.
 * 
 * @param {String} transactionId
 * @param {Function(err)} callback
 * @return {Client} this
 */
Client.prototype.abortTransaction = function (transactionId, callback) {
  var body = AbortTransactionRequest.serialize({transactionId: transactionId});
  this.request('AbortTransaction', body, callback);
  return this;
};

Client.prototype.convertToPutRowParameter = function (tableName, primaryKeys, columns, checking) {
  var row = {
    primaryKeys: [],
    columns: []
  };
  this.addColumns(row.primaryKeys, primaryKeys);
  this.addColumns(row.columns, columns);
  
  return {
    tableName: tableName,
    rowChange: {
      row: row,
      checkingType: checking || 'NO'
    }
  };
};

/**
 * Insert or update a data row.
 * 
 * @param  {String} tableName     
 * @param  {Array|Object} primaryKeys   
 * @param  {Array} columns       
 * @param  {String} checking, 'NO', 'INSERT', 'UPDATE', default is 'NO'.
 * @param  {String} transactionId
 * @param  {Function(err, result)} callback
 * @return {Client} this
 */
Client.prototype.putRow = 
Client.prototype.putData = function (tableName, primaryKeys, columns, checking, transactionId, callback) {
  if (typeof checking === 'function') {
    callback = checking;
    checking = null;
    transactionId = null;
  } else if (typeof transactionId === 'function') {
    callback = transactionId;
    transactionId = null;
  }

  var request = {
    putRowParameter: this.convertToPutRowParameter(tableName, primaryKeys, columns, checking)
  };

  if (transactionId) {
    request.transactionId = transactionId;
  }
  var body = PutRowRequest.serialize(request);
  this.request('PutRow', body, callback);
  return this;
};

/**
 * Multi put rows.
 * @param {String} tableName
 * @param {Array} items
 *  - {Object} item
 *   - {Array|Object} primaryKeys
 *   - {Array|Object} columns
 *   - {String} checking
 * @param {Function(err, result)} callback
 * @return {Client} this
 */
Client.prototype.multiPutRow = function (tableName, items, callback) {
  var parameters = [];
  for (var i = 0; i < items.length; i++) {
    var item = items[i];
    parameters.push(this.convertToPutRowParameter(tableName, item.primaryKeys, item.columns, item.checking));
  }
  var request = {
    parameters: parameters,
  };
  var body = MultiPutRowRequest.serialize(request);
  this.request('MultiPutRow', body, function (err, result) {
    if (err) {
      return callback(err);
    }
    callback(null, result && result.messages || []);
  });
  return this;
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
 * @param {String} [transactionId], transaction id which return by StartTransactionAPI().
 * @param {Function(err, row)} callback 
 * @return {Client} this
 */
Client.prototype.getRow = function (tableName, primaryKeys, columnNames, transactionId, callback) {
  if (typeof columnNames === 'function') {
    callback = columnNames;
    columnNames = null;
    transactionId = null;
  } else if (typeof transactionId === 'function') {
    callback = transactionId;
    transactionId = null;
  }
  var keys = [];
  this.addColumns(keys, primaryKeys);
  
  var request = {
    getRowParameter: {
      tableName: tableName,
      primaryKeys: keys,
      columnNames: columnNames,
    }
  };
  if (transactionId) {
    request.transactionId = transactionId;
  }
  var body = GetRowRequest.serialize(request);
  var self = this;
  self.request('GetRow', body, function (err, result) {
    if (err) { 
      return callback(err, result); 
    }
    callback(err, self.parseRow(result && result.row));
  });
  return this;
};

Client.prototype.parseRows = function (result) {
  var items = result && result.items || [];
  for (var i = 0; i < items.length; i++) {
    var item = items[i];
    item.row = this.parseRow(item.row);
  }
  return items;
};

/**
 * Multi get rows. 10 rows limit.
 * 
 * @param {String} tableName
 * @param {Array} pks primary keys list. [pk1, pk2, ...]
 *  - {Object} one pk like [{Name: 'uid', Value: 'mk2'}, {Name: 'firstname', Value: 'haha'}]
 * @param {Array} [columns]
 * @param {Function(err, rows)} callback
 * @return {Client} this
 */
Client.prototype.multiGetRow = function (tableName, pks, columns, callback) {
  if (typeof columns === 'function') {
    callback = columns;
    columns = null;
  }

  var parameters = [];
  for (var i = 0; i < pks.length; i++) {
    var keys = [];
    this.addColumns(keys, pks[i]);
    parameters.push({
      tableName: tableName,
      primaryKeys: keys,
      columnNames: columns,
    });
  }
  var request = {
    parameters: parameters
  };
  var body = MultiGetRowRequest.serialize(request);
  var self = this;
  self.request('MultiGetRow', body, function (err, result) {
    if (err) {
      return callback(err, result);
    }
    callback(null, self.parseRows(result));
  });
  return this;
};

/**
 * Get multi rows from a table or view by primary key range.
 * 
 * @param {String} tableName the table name.
 * @param {Array|Object|NULL} primaryKeys list of primary keys.
 *  - {Object} primaryKey: {
 *    Name: 'id', Value: 1000
 *  }
 * @param {Object} rangeKey e.g.:
 *  - { Name: 'id', Begin: 1000, End: 1010 }
 *  - { Name: 'id', Begin: exports.INT_MIN, End: exports.INT_MAX }
 *  - { Name: 'name', Begin: exports.STR_MIN, End: exports.STR_MAX }
 *  - { Name: 'enable', Begin: exports.BOOL_MIN, End: exports.BOOL_MAX }
 * @param {Array|null} columnNames, `null` return all columns.
 * @param {Object} [options]
 *  - {Boolean} [isReverse] dafault is false.
 *  - {Number} [limit] return max limit rows, default return all match rows.
 *  - {String} [nextToken]
 *  - {String} [transactionId] transaction id which return by startTransaction().
 * @param {Function(err, row)} callback 
 * @return {Client} this
 */
Client.prototype.getRowsByRange = 
function (tableName, primaryKeys, rangeKey, columnNames, options, callback) {
  if (typeof columnNames === 'function') {
    callback = columnNames;
    columnNames = null;
    options = null;
  } else if (typeof options === 'function') {
    callback = options;
    options = null;
  }

  options = options || {};
  
  var keys = [];
  primaryKeys && this.addColumns(keys, primaryKeys);

  var request = {
    tableName: tableName,
    primaryKeyPrefix: keys,
    rangeKeyName: rangeKey.Name,
    rangeBegin: this.convertToColumnValue({ Value: rangeKey.Begin }),
    rangeEnd: this.convertToColumnValue({ Value: rangeKey.End }),
    columnNames: columnNames,
    isReverse: options.isReverse || false,
  };
  if (options.limit) {
    request.limit = options.limit;
  }
  if (options.nextToken) {
    request.nextToken = options.nextToken;
  }
  if (options.transactionId) {
    request.transactionId = options.transactionId;
  }
  
  var body = GetRowsByRangeRequest.serialize(request);
  console.log('%j, %j', request, GetRowsByRangeRequest.parse(body))
  var self = this;
  this.request('GetRowsByRange', body, function (err, result) {
    if (err) {
      return callback(err);
    }
    callback(err, self.parseRows(result));
  });
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

Client.prototype.convertToDeleteRowParameter = function (tableName, primaryKeys, columnNames) {
  var keys = [];
  this.addColumns(keys, primaryKeys);
  return {
    tableName: tableName,
    rowChange: {
      primaryKeys: keys,
      columnNames: columnNames
    }
  };
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
 * @param {String} [transactionId], transaction id which return by StartTransactionAPI().
 * @param {Function(err, row)} callback 
 * @return {Client} this
 */
Client.prototype.deleteData = 
Client.prototype.deleteRow = function (tableName, primaryKeys, columnNames, transactionId, callback) {
  if (typeof columnNames === 'function') {
    callback = columnNames;
    columnNames = null;
    transactionId = null;
  } else if (typeof transactionId === 'function') {
    callback = transactionId;
    transactionId = null;
  }

  var request = {
    deleteRowParameter: this.convertToDeleteRowParameter(tableName, primaryKeys, columnNames)
  };
  if (transactionId) {
    request.transactionId = transactionId;
  }
  var body = DeleteRowRequest.serialize(request);
  this.request('DeleteRow', body, callback);
};

/**
 * Multi delete rows.
 * @param {String} tableName
 * @param {Array} items
 *  - {Object} item
 *   - {Array|Object} primaryKeys
 *   - {Array|NULL} [columnNames]
 * @param {Function} callback [description]
 * @return {[type]} [description]
 */
Client.prototype.multiDeleteRow = function (tableName, items, callback) {
  var parameters = [];
  for (var i = 0; i < items.length; i++) {
    var item = items[i];
    parameters.push(this.convertToDeleteRowParameter(tableName, item.primaryKeys, item.columnNames));
  }
  var request = {
    parameters: parameters
  };
  var body = MultiDeleteRowRequest.serialize(request);
  this.request('MultiDeleteRow', body, function (err, result) {
    if (err) {
      return callback(err);
    }
    callback(null, result && result.messages || []);
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
Client.prototype.batchModifyData = 
Client.prototype.batchModifyRow = function (tableName, modifyItems, transactionID, callback) {
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

Client.prototype.addColumns = function (columns, items) {
  if (!Array.isArray(items)) {
    items = [items];
  }
  for (var i = 0, l = items.length; i < l; i++) {
    var item = items[i];
    var value = this.convertToColumnValue(item);
    columns.push({name: item.Name, value: value});
  }
};

/**
 * Convert javascript data to OTS ColumnValue data. 
 * 
 * { Name: 'uid', Value: 'foo' } => { valueS: "'foo'", type: 'STRING' }
 * { Name: 'id', Value: 123 } => { valueI: 123, type: 'INTEGER' }
 * { Name: 'price', Value: 100.85 } => { valueD: 100.85, type: 'DOUBLE' }
 * { Name: 'enable', Value: true } => { valueB: true, type: 'BOOLEAN' }
 * 
 * @param  {Object} o
 *  - {String} Value
 * @return {Object} the OTS data contain `Type`. 
 */
Client.prototype.convertToColumnValue = function (o) {
  var value = o.Value;
  var type = 'STRING';
  var field = 'valueS';
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
    field = 'valueI';
    break;
  case exports.INT_MAX:
    value = 'INF_MAX';
    type = 'INTEGER';
    field = 'valueI';
    break;
  case exports.BOOL_MIN:
    value = 'INF_MIN';
    type = 'BOOLEAN';
    field = 'valueB';
    break;
  case exports.BOOL_MAX:
    value = 'INF_MAX';
    type = 'BOOLEAN';
    field = 'valueB';
    break;
  default:
    var t = typeof value;
    if (t === 'string') {
      // TODO
    } else if (t === 'boolean') {
      type = 'BOOLEAN';
      field = 'valueB';
    } else if (t === 'number') {
      if (value % 1 === 0) {
        type = 'INTEGER';
        field = 'valueI';
      } else {
        type = 'DOUBLE';
        field = 'valueD';
      }
    } else {
      value = String(value);
    }
    break;
  }
  var cv = {type: type};
  cv[field] = value;
  return cv;
};

Client.ValueParsers = {
  DOUBLE: parseFloat,
  INTEGER: parseInt,
  BOOLEAN: function (o) {
    return o === 'TRUE';
  }
};

Client.prototype.parseColumnValue = function (col) {
  var val = col.value;
  var v = null;
  if (val.type === 'STRING') {
    v = val.valueS;
  } else if (val.type === 'INTEGER') {
    v = val.valueI;
  } else if (val.type === 'DOUBLE') {
    v = val.valueD;
  } else if (val.type === 'BOOLEAN') {
    v = val.valueB;
  }
  return v;
};

Client.prototype.parseRow = function (row) {
  if (!row) {
    return null;
  }
  var columns = row.columns || [];
  if (!Array.isArray(columns)) {
    columns = [columns];
  }
  if (columns.length === 0) {
    return null;
  }

  var keys = row.primaryKeys;
  var r = {};
  for (var i = 0; i < columns.length; i++) {
    var col = columns[i];
    r[col.name] = this.parseColumnValue(col);
  }
  for (var i = 0; i < keys.length; i++) {
    var col = keys[i];
    r[col.name] = this.parseColumnValue(col);
  }
  return r;
};

/**
 * Signature the params.
 * 
 * Base64(hmac-sha1(VERB + “\n” + “KEY1=VALUE1” + “&” + “KEY2=VALUE2”, AccessKey))
 *
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
 * 请求签名的计算方法如下：

Signature = base64(HmacSha1(StringToSign));
StringToSign = CanonicalURI + '\n' + HTTPRequestMethod + '\n' + CanonicalQueryString + '\n' + CanonicalHeaders + '\n'
CanonicalQueryString = URLEncodedName1 + '=' + URLEncodedValue1 + '&' + ... + URLEncodedNameN + '=' + URLEncodedValueN
CanonicalHeaders = LowerCase(HeaderName1) + ':' + Trim(HeaderValue1) + '\n' + ... + LowerCase(HeaderNameN) + ':' + Trim(HeaderValueN)
CanonicalURI 为 HTTP URL中的绝对路径部分， 如"http://ots.aliyuncs.com/ListTable"中， CanonicalURI 为"/ListTable"。
HTTPRequestMethod为HTTP请求方法， 如GET、POST或PUT等， 注意必须全大写。

CanonicalQueryString 为将URL中的查询参数按照以下规则构造的字符串：
<1> 查询参数中的每个参数的名称和值都必须是URL encode（注意空格在URL encode之后为%20，不能为'+'）之后的内容。
<2> 按照每个参数的名称和值对所有参数依照从小到大的顺序进行排序， 排序按照严格的字符顺序。
<3> 按照参数排序后的结果， 将所有参数组合成一个字符串， 参数的名称和值之间用'='相隔，参数之间用'&'相隔。
CanonicalHeaders 为所有的OTS标准HTTP header按照以下规则构造的字符串（不包括x-ots-signature）：
<1> 需要包含且只包含所有以'x-ots-'开头的OTS标准头
<2> header名称全部小写， 值必须经过trim
<3> header按照名字的字母序从小到大排序
<4> header的名称和值之间以':'相隔
<5> 每个header之间以'\n'相隔

最后计算出来的签名需要放在HTTP头中， 头的名称为"x-ots-signature"。

 * @param {Buffer} body
 * @return {object} request headers
 */
Client.prototype._make_headers = function (body, canonicalURI) {
  var bodyMD5 = utility.md5(body, 'base64');
  var headers = {
    'x-ots-date': new Date().toGMTString(),
    'x-ots-apiversion': String(this.APIVersion),
    'x-ots-accesskeyid': this.accessID,
    'x-ots-signaturemethod': this.signatureMethod,
    'x-ots-signatureversion': String(this.signatureVersion),
    'x-ots-contentmd5': bodyMD5,
  };
  var sorted = [];
  for (var k in headers) {
    var v = headers[k];
    sorted.push(k + ':' + v.trim());
  }
  sorted.sort();
  var basestring = canonicalURI + '\nPOST\n\n' + sorted.join('\n') + '\n';
  headers['x-ots-signature'] = utility.hmac('sha1', this.accessKey, basestring, 'base64');
  debug('basestring: %s', basestring);
  return headers;
};

/**
 * Request the OTS
 * 
 * @param {String} optype, operate type, eg: 'CreateTable', 'DeleteTable'
 * @param {Buffer} body
 * @param {Function(err, result)} callback
 * @return {Client} client instance.
 */
Client.prototype.request = function (optype, body, callback) {
  var url = '/' + optype;
  var self = this;
  body = body || '';
  var headers = self._make_headers(body, url);
  var APIHostInfo = self.APIHostInfo;
  var hostname = APIHostInfo.hostname;
  debug('%s%s body size: %d, headers: %j', hostname, url, body.length, headers);
  self.dns.resolve4(hostname, function (err, addresses) {
    if (err) {
      err.hostname = hostname;
      return callback(err);
    }
    hostname = addresses[0] || hostname;
    hostname = APIHostInfo.protocol + '//' + hostname;
    if (APIHostInfo.port) {
      hostname += ':' + APIHostInfo.port;
    }
    url = hostname + url;

    urllib.request(url, {
      headers: headers,
      content: body,
      type: 'POST',
      timeout: self.requestTimeout,
      agent: self.requestAgent
    }, function (err, buf, res) {
      res = res || {};
      var resHeaders = res.headers || {};
      var statusCode = res.statusCode || -1;
      var serverId = 'RequestID: ' + resHeaders['x-ots-requestid'] + ' HostID: ' + resHeaders['x-ots-hostid'];
      debug('%s %s res headers: %j, serverId: %s', url, statusCode, resHeaders, serverId);
      if (err) {
        err.name = 'OTS' + err.name;
        err.url = url + '?' + body;
        err.serverId = serverId;
        return callback(err, null, res);
      }

      if (statusCode !== 200) {
        var e = ErrorMessage.parse(buf);
        err = new Error(e.message);
        err.name = e.code + 'Error';
        err.code = e.code;
        err.serverId = serverId;
        return callback(err, null, res);
      }

      var pbResponse = ResponseMap[optype];
      if (!pbResponse) {
        return callback(null, null, res);
      }

      var result = pbResponse.parse(buf);
      
      // if (result.Error) {
      //   err = new Error(result.Error.Message);
      //   err.name = result.Error.Code || 'OTSError';
      //   err.url = url + '?' + body;
      //   err.data = result;
      // } else {
      //   var key = optype + 'Result';
      //   result = result[key] || result;
      // }
      callback(err, result, res);
    });
  });
};

Client.prototype.close = function () {
  this.dns.close();
};

/**
 * Create a client.
 * @param  {Object} options
 * @return {Client}
 */
module.exports.createClient = function (options) {
  return new Client(options);
};
