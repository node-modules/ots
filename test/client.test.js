/*!
 * ots - test/client.test.js
 * Copyright(c) 2012 - 2013 fengmk2 <fengmk2@gmail.com> (http://fengmk2.github.com/)
 * MIT Licensed
 */

/**
 * Module dependencies.
 */

var should = require('should');
var utility = require('utility');
var EventProxy = require('eventproxy').EventProxy;
var crypto = require('crypto');
var mm = require('mm');
var pedding = require('pedding');
var ots = require('../');
var config = require('./config.js');

describe('client.test.js', function() {
  var client = ots.createClient({
    accessID: config.accessID,
    accessKey: config.accessKey,
    APIHost: config.APIHost
  });

  afterEach(mm.restore);

  before(function (done) {
    var ep = EventProxy.create('testgroup', 'test', 'testuser', 'testurl', 'testuser_range', function () {
      done();
    });
    client.deleteTableGroup('testgroup', function (err) {
      ep.emit('testgroup');
    });
    client.deleteTable('test', function (err) {
      ep.emit('test');
    });
    client.createTable({
      TableName: 'testuser',
      PrimaryKey: [
        { 'Name': 'uid', 'Type': 'STRING' },
        { 'Name': 'firstname', 'Type': 'STRING' },
      ],
    }, function (err, result) {
      ep.emit('testuser')
    });

    client.createTable({
      TableName: 'testuser_range',
      PrimaryKey: [
        { Name: 'uid_md5', Type: 'STRING' },
        { Name: 'uid', Type: 'STRING' },
        { Name: 'create_time', Type: 'STRING' }
      ]
    }, function (err, result) {
      ep.emit('testuser_range');
    });

    client.createTable({
      TableName: 'testurl',
      PrimaryKey: [
        { 'Name': 'md5', 'Type': 'STRING' },
      ],
    }, function (err, result) {
      ep.emit('testurl')
    });
  });

  describe('createTableGroup()', function () {
    it('should create a group success', function (done) {
      client.createTableGroup('testgroup', 'STRING', function (err) {
        should.not.exist(err);
        client.createTableGroup('testgroup', 'STRING', function (err) {
          should.exist(err);
          err.name.should.equal('OTSStorageObjectAlreadyExistError');
          err.message.should.equal('Requested table/view does exist.');
          err.code.should.equal('OTSStorageObjectAlreadyExist');
          done();
        });
      });
    });

    it('should create a group with wrong key type', function (done) {
      client.createTableGroup('testgroup', 'BOOLEAN', function (err) {
        should.exist(err);
        err.name.should.equal('OTSParameterInvalidError');
        err.message.should.equal('BOOLEAN is an invalid type for the first column of primary key (partition key).');
        done();
      });
    });
  });

  describe('listTableGroup()', function () {
    it('should list all groups', function (done) {
      client.listTableGroup(function (err, groups) {
        should.not.exist(err);
        groups.should.be.an.instanceof(Array);
        groups.length.should.above(0);
        // groups.should.include('testgroup');
        done();
      });
    });
  });

  describe('deleteTableGroup()', function () {
    before(function (done) {
      client.createTableGroup('testgroup', 'STRING', function (err) {
        done();
      });
    });

    it('should delete a group', function (done) {
      client.deleteTableGroup('testgroup', function (err) {
        should.not.exist(err);
        client.deleteTableGroup('testgroup', function (err) {
          should.exist(err);
          err.name.should.equal('OTSStorageObjectNotExistError');
          err.message.should.equal('Requested table/view doesn\'t exist.');
          done();
        });
      });
    });
  });

  describe('createTable()', function () {

    it('should return OTSParameterInvalidError when missing primary key', function (done) {
      client.createTable({ TableName: 'test' }, function (err, result) {
        should.exist(err);
        err.name.should.equal('OTSParameterInvalidError');
        err.message.should.equal('The Table/View does not specify the primary key.');
        done();
      });
    });

    it('should create "test" table success', function(done) {
      client.createTable({
        TableName: 'test',
        PrimaryKey: [
          {'Name': 'uid', 'Type': 'STRING'},
        ],
        View: [
          { 
            'Name': 'view1', 
            'PrimaryKey' : [
              {'Name':'uid', 'Type':'STRING'},
              {'Name':'flag', 'Type':'STRING'},
              {'Name':'docid', 'Type':'STRING'},
            ],
            'Column' : [
              {'Name':'updatetime', 'Type':'STRING'},
              {'Name':'createtime', 'Type':'STRING'},
            ],
          }
        ]
      }, function (err) {
        should.not.exist(err);
        done();
      });
    });

    it('should get "test" table meta success', function (done) {
      client.getTableMeta('test', function (err, meta) {
        should.not.exist(err);
        // console.log('%j', meta)
        meta.should.have.keys([ 'tableName', 'primaryKeys', 'views' ]);
        meta.tableName.should.equal('test');
        meta.primaryKeys[0].should.have.keys([ 'name', 'type' ]);
        // meta.views.PrimaryKey.should.length(3);
        // meta.views.Column.should.length(2);
        // meta.views.Name.should.equal('view1');
        done();
      });
    });

    it('should list table success', function (done) {
      client.listTable(function (err, tablenames) {
        should.not.exist(err);
        tablenames.should.be.an.instanceof(Array);
        tablenames.should.include('test');
        done();
      });
    });

    it('should create "test" table exist error', function (done) {
      client.createTable({ 
        TableName: 'test', 
        PrimaryKey: [ { Name: 'id', Type: 'STRING' } ] 
      }, function (err, result) {
        should.exist(err);
        err.name.should.equal('OTSStorageObjectAlreadyExistError');
        err.message.should.equal('Requested table/view does exist.');
        done();
      });
    });

    it('should delete "test" table success and error', function (done) {
      client.deleteTable('test', function (err) {
        should.not.exist(err);
        client.deleteTable('test', function (err) {
          should.exist(err);
          err.name.should.equal('OTSStorageObjectNotExistError');
          err.message.should.equal('Requested table/view doesn\'t exist.');
          done();
        });
      });
    });

  });

  describe('Transaction', function () {
    var transactionID = null;
    describe('startTransaction()', function () {
      it('should start and get a transaction id', function (done) {
        client.startTransaction('testuser', 'foo', function (err, tid) {
          should.not.exist(err);
          tid.should.be.a('string');
          transactionID = tid;
          done();
        });
      });
    });

    describe('commitTransaction()', function () {
      it('should commit a transaction', function (done) {
        client.commitTransaction(transactionID, function (err) {
          should.not.exist(err);
          done();
        });
      });
      it('should OTSParameterInvalid when commit a error tranID', function (done) {
        client.commitTransaction('errorTransactionID', function (err) {
          should.exist(err);
          err.name.should.equal('OTSParameterInvalidError');
          err.message.should.equal('TransactionID is invalid.');
          done();
        });
      });
    });

    describe('abortTransaction()', function () {
      it('should abort a transaction success', function (done) {
        client.startTransaction('testuser', 'foo-need-to-abort', function (err, tid) {
          client.abortTransaction(tid, function (err) {
            should.not.exist(err);
            done();
          });
        });
      });

      it('should OTSStorageSessionNotExist when abort a committed tran', function (done) {
        client.abortTransaction(transactionID, function (err) {
          should.exist(err);
          err.name.should.equal('OTSStorageSessionNotExistError');
          done();
        });
      });

      it('should OTSParameterInvalid when abort a error tranID', function (done) {
        client.abortTransaction('errorTransactionID', function (err) {
          should.exist(err);
          err.name.should.equal('OTSParameterInvalidError');
          err.message.should.equal('TransactionID is invalid.');
          done();
        });
      });
    });
  });

  var now = new Date();
  describe('putData()', function () {
    it('should insert a row success', function (done) {
      client.putData('testuser', 
        [ 
          { Name: 'uid', Value: 'mk2' }, 
          { Name: 'firstname', Value: 'yuan' },
        ],
        [
          { Name: 'lastname', Value: 'feng\' mk2' },
          { Name: 'nickname', Value: '  苏千\n ' },
          { Name: 'age', Value: 28 }, // int64
          { Name: 'json', Value: '{ "foo": "bar" }' },
          { Name: 'price', Value: 110.5 },
          { Name: 'enable', Value: true },
          { Name: 'man', Value: true },
          { Name: 'status', Value: null },
          { Name: 'female', Value: false },
          { Name: 'createtime', Value: now.toJSON() },
        ], 
      function (err) {
        should.not.exist(err);
        done();
      });
    });

    it('should UPDATE a row error when pk not exists', function (done) {
      client.putData('testuser', 
        [ 
          { Name: 'uid', Value: 'mk2222' }, 
          { Name: 'firstname', Value: 'yuannot-exsits' },
        ],
        [
          { Name: 'lastname', Value: 'feng\' mk2' },
          { Name: 'nickname', Value: '  苏千\n ' },
          { Name: 'age', Value: 28 }, // int64
          { Name: 'json', Value: '{ "foo": "bar" }' },
          { Name: 'price', Value: 110.5 },
          { Name: 'enable', Value: true },
          { Name: 'man', Value: true },
          { Name: 'status', Value: null },
          { Name: 'female', Value: false },
          { Name: 'createtime', Value: now.toJSON() },
        ], 
        'UPDATE',
      function (err) {
        should.exist(err);
        err.name.should.equal('OTSStoragePrimaryKeyNotExistError');
        err.message.should.equal("Row to update doesn't exist.");
        done();
      });
    });

    it('should INSERT a row error when pk exists', function (done) {
      client.putData('testuser', 
        [ 
          { Name: 'uid', Value: 'mk2' }, 
          { Name: 'firstname', Value: 'yuan' },
        ],
        [
          { Name: 'lastname', Value: 'feng\' mk2' },
          { Name: 'nickname', Value: '  苏千\n ' },
          { Name: 'age', Value: 28 }, // int64
          { Name: 'json', Value: '{ "foo": "bar" }' },
          { Name: 'price', Value: 110.5 },
          { Name: 'enable', Value: true },
          { Name: 'man', Value: true },
          { Name: 'status', Value: null },
          { Name: 'female', Value: false },
          { Name: 'createtime', Value: now.toJSON() },
        ], 
        'INSERT',
      function (err) {
        should.exist(err);
        err.name.should.equal('OTSStoragePrimaryKeyAlreadyExistError');
        err.message.should.equal("Row to insert does exist.");
        done();
      });
    });
  });

  describe('getRow()', function () {
    before(function (done) {
      client.putData('testuser', 
        [ 
          { Name: 'uid', Value: 'mk2' }, 
          { Name: 'firstname', Value: 'yuan' },
        ],
        [
          { Name: 'lastname', Value: 'feng\' mk2' },
          { Name: 'nickname', Value: '  苏千\n ' },
          { Name: 'age', Value: 28 }, // int64
          { Name: 'json', Value: '{ "foo": "bar" }' },
          { Name: 'price', Value: 110.5 },
          { Name: 'enable', Value: true },
          { Name: 'man', Value: true },
          { Name: 'status', Value: null },
          { Name: 'female', Value: false },
          { Name: 'createtime', Value: now.toJSON() },
          { Name: 'haha', Value: '哈哈' },
        ], 
      function (err) {
        should.not.exist(err);
        done();
      });
    });
    
    it('should return error pk not match', function (done) {
      done = pedding(2, done);

      client.getRow('testuser', 
      [ 
        { Name: 'uid1', Value: 'mk2' }, 
        { Name: 'firstname', Value: 'yuan' },
      ], 
      function (err, row) {
        should.exist(err);
        err.name.should.include('OTSMetaNotMatchError');
        err.message.should.equal('Primary key meta defined in the request does not match with the Table meta.');
        err.should.have.property('serverId');
        should.not.exist(row);
        done();
      });

      // 顺序错误...
      client.getRow('testuser', 
      [ 
        { Name: 'firstname', Value: 'yuan' },
        { Name: 'uid', Value: 'mk2' }, 
      ], 
      function (err, row) {
        should.exist(err);
        err.name.should.include('OTSMetaNotMatchError');
        err.message.should.equal('Primary key meta defined in the request does not match with the Table meta.');
        err.should.have.property('serverId');
        should.not.exist(row);
        done();
      });
    });

    it('should return a row all columns', function (done) {
      client.getRow('testuser', 
      [ 
        { Name: 'uid', Value: 'mk2' }, 
        { Name: 'firstname', Value: 'yuan' },
      ], 
      function (err, row) {
        should.not.exist(err);
        row.should.have.keys([ 
          'uid', 'firstname', // should include pk
          'lastname', 'nickname',
          'age', 'price', 'enable',
          'man', 'female', 
          'json', 'status',
          'createtime',
          'haha'
        ]);
        row.uid.should.equal('mk2');
        row.firstname.should.equal('yuan');
        row.lastname.should.equal('feng\' mk2');
        row.nickname.should.equal('  苏千\n ');
        row.age.should.equal('28'); // int64, will be auto convert to string by protobuf module
        row.price.should.equal(110.5);
        row.enable.should.equal(true);
        row.man.should.equal(true);
        row.female.should.equal(false);
        row.status.should.equal('null');
        row.createtime.should.equal(now.toJSON());
        row.json.should.equal('{ "foo": "bar" }');
        row.haha.should.equal('哈哈');
        done();
      });
    });

    it('should return a row some columns', function (done) {
      client.getRow('testuser', 
      [ 
        { Name: 'uid', Value: 'mk2' }, 
        { Name: 'firstname', Value: 'yuan' },
      ], 
      ['uid', 'json'],
      function (err, row) {
        should.not.exist(err);
        row.should.have.keys([ 
          'uid',
          'json',
        ]);
        row.uid.should.equal('mk2');
        row.json.should.equal('{ "foo": "bar" }');
        done();
      });
    });

    it('should return a row some columns and not exists columns', function (done) {
      done = pedding(6, done);

      client.getRow('testuser', 
      [ 
        { Name: 'uid', Value: 'mk2' }, 
        { Name: 'firstname', Value: 'yuan' },
      ], 
      ['uid', 'json', 'notexists'],
      function (err, row) {
        should.not.exist(err);
        row.should.have.keys([ 
          'uid',
          'json',
        ]);
        row.uid.should.equal('mk2');
        row.json.should.equal('{ "foo": "bar" }');
        done();
      });

      client.getRow('testuser', 
      [ 
        { Name: 'uid', Value: 'mk2' }, 
        { Name: 'firstname', Value: 'yuan' },
      ], 
      ['json', 'notexists'],
      function (err, row) {
        should.not.exist(err);
        row.should.have.keys([ 
          'json',
        ]);
        row.json.should.equal('{ "foo": "bar" }');
        done();
      });

      client.getRow('testuser', 
      [ 
        { Name: 'uid', Value: 'mk2' }, 
        { Name: 'firstname', Value: 'yuan' },
      ], 
      ['notjson', 'notexists'],
      function (err, row) {
        should.not.exist(err);
        should.not.exist(row);
        done();
      });

      client.getRow('testuser', 
      [ 
        { Name: 'uid', Value: 'mk2' }, 
        { Name: 'firstname', Value: 'yuan' },
      ], 
      ['uid', 'notexists'],
      function (err, row) {
        should.not.exist(err);
        row.should.eql({uid: 'mk2'});
        done();
      });

      client.getRow('testuser', 
      [ 
        { Name: 'uid', Value: 'mk2' }, 
        { Name: 'firstname', Value: 'yuan' },
      ], 
      ['firstname', 'notexists'],
      function (err, row) {
        should.not.exist(err);
        row.should.eql({firstname: 'yuan'});
        done();
      });

      client.getRow('testuser', 
      [ 
        { Name: 'uid', Value: 'mk2' }, 
        { Name: 'firstname', Value: 'yuan' },
      ], 
      ['firstname', 'uid'],
      function (err, row) {
        should.not.exist(err);
        row.should.eql({firstname: 'yuan', uid: 'mk2'});
        done();
      });
    });

    it('should return null when pk not exists', function (done) {
      client.getRow('testuser', 
      [ 
        { Name: 'uid', Value: 'not-existskey' }, 
        { Name: 'firstname', Value: 'haha' },
      ], function (err, row) {
        should.not.exist(err);
        should.not.exist(row);
        done();
      });
    });
  });

  describe('getRowsByRange()', function () {
    before(function (done) {
      var ep = EventProxy.create();
      ep.after('putDataDone', 10, function () {
        // { Name: 'uid_md5', Type: 'STRING' },
        // { Name: 'uid', Type: 'STRING' },
        // { Name: 'create_time', Type: 'STRING' }
        client.getRow('testuser_range', [
          { Name: 'uid_md5', Value: 'md51' }, 
          { Name: 'uid', Value: '320' },
          { Name: 'create_time', Value: '2013090' },
        ], function (err, row) {
          should.not.exist(err);
          done();
        });
      });
      for (var i = 0; i < 10; i++) {
        client.putRow('testuser_range', 
        [ 
          { Name: 'uid_md5', Value: 'md51' }, 
          { Name: 'uid', Value: '320' }, 
          { Name: 'create_time', Value: '201309' + i } 
        ],
        [
          { Name: 'lastname', Value: 'lastname' + i },
          { Name: 'nickname', Value: '花名' + i },
          { Name: 'age', Value: 20 + i },
          { Name: 'price', Value: 50.5 + i },
          { Name: 'enable', Value: i % 2 === 0 },
          { Name: 'man', Value: i % 2 === 0 },
          { Name: 'female', Value: i % 3 === 0 },
          { Name: 'modified', Value: new Date().toJSON() },
        ], function (err, result) {
          should.not.exist(err);
          ep.emit('putDataDone');
        });
      }
    });

    var nextBegin = null;
    it('should get top 6 rows, limit 6', function (done) {
      client.getRowsByRange('testuser_range', 
        [ 
          { Name: 'uid_md5', Value: 'md51' }, 
          { Name: 'uid', Value: '320' }, 
        ],
        { Name: 'create_time', Begin: ots.STR_MIN, End: ots.STR_MAX }, null, {limit: 6},
      function (err, rows) {
        should.not.exist(err);
        rows.should.length(6);
        for (var i = rows.length; i--; ) {
          var row = rows[i];
          row.should.have.keys([ 
            'uid', 'uid_md5', 'lastname', 'nickname', 'age', 'price',
            'enable', 'man', 'female', 'create_time', 'modified'
          ]);
        }
        nextBegin = rows[rows.length - 1].firstname;
        done();
      });
    });

    it('should get 2 rows[6-8) limit 10', function (done) {
      client.getRowsByRange('testuser_range', 
        [
          { Name: 'uid_md5', Value: 'md51' }, 
          { Name: 'uid', Value: '320' }, 
        ], 
        { Name: 'create_time', Begin: '2013096', End: '2013098' }, ['age'], {limit: 10}, 
      function (err, rows) {
        should.not.exist(err);
        rows.should.length(2);
        for (var i = rows.length; i--; ) {
          var row = rows[i];
          row.should.have.keys('age');
        }
        nextBegin = rows[rows.length - 1].md5;
        done();
      });
    });

    it('should get 0 rows', function (done) {
      client.getRowsByRange('testuser_range', 
        [
          { Name: 'uid_md5', Value: 'md51' }, 
          { Name: 'uid', Value: '320' }, 
        ], 
        { Name: 'create_time', Begin: '2013106', End: '2013108' }, ['age'], {limit: 10}, 
      function (err, rows) {
        should.not.exist(err);
        rows.should.length(0);
        done();
      });
    });
  });

  describe('deleteRow()', function () {
    it('should delete a row', function (done) {
      client.deleteData('testuser', 
        [
          {Name: 'uid', Value: 'mk2'},
          {Name: 'firstname', Value: 'yuan'}
        ],
      function (err) {
        should.not.exist(err);
        // TODO: WTF, delete delay?!
        client.getRow('testuser', [
            {Name: 'uid', Value: 'mk2'},
            {Name: 'firstname', Value: 'yuan'}
          ],
        function (err) {
          should.not.exist(err);
          done();
        });
      });
    });

    it('should delete by a not exists key', function (done) {
      client.deleteRow('testuser', [
        {Name: 'uid', Value: 'not-existskey'},
        {Name: 'firstname', Value: 'yuan'}
      ], function (err) {
        should.not.exist(err);
        done();
      });
    });

    it('should delete row with wrong pk', function (done) {
      client.deleteRow('testuser', [
        {Name: 'uid2', Value: 'not-existskey'},
        {Name: 'firstname', Value: 'yuan'}
      ], function (err) {
        should.exist(err);
        err.name.should.equal('OTSMetaNotMatchError');
        err.message.should.equal('Primary key meta defined in the request does not match with the Table meta.');
        done();
      });
    });
  });

  describe('batchModifyRow()', function () {
    var url = 'http://t.cn/abc' + new Date().getTime();
    var urlmd5 = utility.md5(url);
    var transactionID = null;

    after(function (done) {
      client.abortTransaction(transactionID, function (err) {
        done();
      });
    });

    it('should delete "' + url + '" and insert new', function (done) {
      client.startTransaction('testurl', urlmd5, function (err, tid) {
        should.not.exist(err);
        tid.should.be.a('string');
        transactionID = tid;
        client.batchModifyData('testurl', 
        [
          {
            Type: 'DELETE',
            PrimaryKeys: {Name: 'md5', Value: urlmd5}
          },
          {
            Type: 'PUT',
            PrimaryKeys: {Name: 'md5', Value: urlmd5},
            Columns: [
              {Name: 'url', Value: url},
              {Name: 'createtime', Value: new Date().toJSON()}
            ],
            Checking: 'NO'
          }
        ], tid, function (err) {
          should.not.exist(err);
          client.commitTransaction(tid, function (err) {
            should.not.exist(err);
            done();
          });
        });
      });
    });
  });

  describe('multiPutRow()', function () {
    it('should multi put 100 rows success', function (done) {
      var items = [];
      for (var i = 0; i < 100; i++) {
        items.push({
          primaryKeys: [ 
            { Name: 'uid', Value: 'testuser_multiPutRow_' + i }, 
            { Name: 'firstname', Value: 'name' + i } 
          ],
          columns: [
            { Name: 'lastname', Value: 'lastname' + i },
            { Name: 'nickname', Value: '花名' + i },
            { Name: 'age', Value: 20 + i },
            { Name: 'price', Value: 50.5 + i },
            { Name: 'enable', Value: i % 2 === 0 },
            { Name: 'man', Value: i % 2 === 0 },
            { Name: 'female', Value: i % 3 === 0 },
            { Name: 'index', Value: i },
            { Name: 'createtime', Value: new Date().toJSON() },
          ]
        });
      }
      client.multiPutRow('testuser', items, function (err, results) {
        should.not.exist(err);
        results.should.length(100);
        results[0].should.eql({code: 'OK'});
        done();
      });
    });

    it('should multi put 101 rows Rows count exceeds the upper limit', function (done) {
      var items = [];
      for (var i = 0; i < 1001; i++) {
        items.push({
          primaryKeys: [ 
            { Name: 'uid', Value: 'testuser_multiPutRow_' + i }, 
            { Name: 'firstname', Value: 'name' + i } 
          ],
          columns: [
            { Name: 'lastname', Value: 'lastname' + i },
            { Name: 'nickname', Value: '花名' + i },
            { Name: 'age', Value: 20 + i },
            { Name: 'price', Value: 50.5 + i },
            { Name: 'enable', Value: i % 2 === 0 },
            { Name: 'man', Value: i % 2 === 0 },
            { Name: 'female', Value: i % 3 === 0 },
            { Name: 'index', Value: i },
            { Name: 'createtime', Value: new Date().toJSON() },
          ]
        });
      }
      client.multiPutRow('testuser', items, function (err, results) {
        should.exist(err);
        err.name.should.equal('OTSParameterInvalidError');
        err.message.should.equal('Rows count exceeds the upper limit');
        should.not.exists(results);
        done();
      });
    });
  });

  describe('multiDeleteRow()', function () {
    it('should multi delete 100 rows, 50 rows OK all columns success', function (done) {
      var items = [];
      for (var i = 50; i < 100; i++) {
        items.push({
          primaryKeys: [ 
            { Name: 'uid', Value: 'testuser_multiPutRow_' + i }, 
            { Name: 'firstname', Value: 'name' + i } 
          ]
        });
      }
      client.multiDeleteRow('testuser', items, function (err, results) {
        should.not.exist(err);
        results.should.length(50);
        results[0].should.eql({code: 'OK'});
        done();
      });
    });

    it('should multi delete 100 rows some columns success', function (done) {
      var items = [];
      for (var i = 0; i < 100; i++) {
        items.push({
          primaryKeys: [ 
            { Name: 'uid', Value: 'testuser_multiPutRow_' + i }, 
            { Name: 'firstname', Value: 'name' + i } 
          ],
          columnNames: ['man', 'age']
        });
      }
      client.multiDeleteRow('testuser', items, function (err, results) {
        should.not.exist(err);
        results.should.length(100);
        results[0].should.eql({code: 'OK'});
        done();
      });
    });

    it('should multi delete OTSParameterInvalidError', function (done) {
      var items = [];
      for (var i = 0; i < 100; i++) {
        items.push({
          primaryKeys: [ 
            { Name: 'uid', Value: 'testuser_multiPutRow_' + i }, 
            { Name: 'firstname', Value: 'name' + i } 
          ],
          columnNames: ['man', 'age-not-exists']
        });
      }
      client.multiDeleteRow('testuser', items, function (err, results) {
        should.exist(err);
        err.name.should.equal('OTSParameterInvalidError');
        err.message.should.equal('Column name age-not-exists is invalid.');
        done();
      });
    });

    it('should multi put 101 rows Rows count exceeds the upper limit', function (done) {
      var items = [];
      for (var i = 0; i < 1001; i++) {
        items.push({
          primaryKeys: [ 
            { Name: 'uid', Value: 'testuser_multiPutRow_' + i }, 
            { Name: 'firstname', Value: 'name' + i } 
          ],
          columnNames: ['man']
        });
      }
      client.multiDeleteRow('testuser', items, function (err, results) {
        should.exist(err);
        err.name.should.equal('OTSParameterInvalidError');
        err.message.should.equal('Rows count exceeds the upper limit');
        should.not.exists(results);
        done();
      });
    });
  });

  describe('multiGetRow()', function () {
    before(function (done) {
      var ep = EventProxy.create();
      ep.after('putDataDone', 5, function () {
        done();
      });
      for (var i = 0; i < 5; i++) {
        client.putData('testuser', 
        [ 
          { Name: 'uid', Value: 'testuser_mget2_' + i }, 
          { Name: 'firstname', Value: 'name' + i } 
        ],
        [
          { Name: 'lastname', Value: 'lastname' + i },
          { Name: 'nickname', Value: '花名' + i },
          { Name: 'age', Value: 20 + i },
          { Name: 'price', Value: 50.5 + i },
          { Name: 'enable', Value: i % 2 === 0 },
          { Name: 'man', Value: i % 2 === 0 },
          { Name: 'female', Value: i % 3 === 0 },
          { Name: 'index', Value: i },
          { Name: 'createtime', Value: new Date().toJSON() },
        ], function (err, result) {
          // should.not.exist(err);
          ep.emit('putDataDone');
        });
      }
    });

    it('should return 5 rows and 5 empty rows with all columns', function (done) {
      var pks = [];
      for (var i = 0; i < 10; i++) {
        pks.push([ 
          { Name: 'uid', Value: 'testuser_mget2_' + i }, 
          { Name: 'firstname', Value: 'name' + i } 
        ]);
      }
      client.multiGetRow('testuser', pks, function (err, items) {
        should.not.exist(err);
        items.should.length(10);
        for (var i = 0; i < 5; i++) {
          var item = items[i];
          item.isSucceed.should.equal(true);
          item.error.should.eql({ code: 'OK' });
          item.tableName.should.equal('testuser');
          item.row.should.have.keys('uid', 'age', 'createtime', 'enable', 'female', 'index', 'lastname',
            'man', 'nickname', 'price', 'firstname');
          item.row.index.should.equal(String(i));
        }
        for (var i = 5; i < 10; i++) {
          var item = items[i];
          item.isSucceed.should.equal(true);
          item.error.should.eql({ code: 'OK' });
          item.tableName.should.equal('testuser');
          should.not.exist(item.row);
        }
        done();
      });
    });

    it('should Rows count exceeds the upper limit error', function (done) {
      var pks = [];
      for (var i = 0; i < 101; i++) {
        pks.push([ 
          { Name: 'uid', Value: 'testuser_' + i }, 
          { Name: 'firstname', Value: 'name' + i } 
        ]);
      }
      client.multiGetRow('testuser', pks, function (err, items) {
        should.exist(err);
        err.name.should.equal('OTSParameterInvalidError');
        err.message.should.equal('Rows count exceeds the upper limit');
        done();
      });
    });

    it('should pk error', function (done) {
      var pks = [];
      for (var i = 0; i < 2; i++) {
        pks.push([ 
          { Name: 'uid2', Value: 'testuser_' + i }, 
          { Name: 'firstname', Value: 'name' + i } 
        ]);
      }
      client.multiGetRow('testuser', pks, function (err, items) {
        should.exist(err);
        err.name.should.equal('OTSMetaNotMatchError');
        err.message.should.equal('Primary key schema from request is not match with table meta: uid2:STRING,firstname:STRING');
        done();
      });
    });
  });

  describe('mock()', function () {
    var _client = ots.createClient({
      accessID: config.accessID,
      accessKey: config.accessKey,
      APIHost: config.APIHost,
      requestTimeout: 1
    });

    after(function () {
      _client.close();
    });

    it('request error', function (done) {
      _client.getRow('testuser', 
      [ 
        { Name: 'uid', Value: 'mk2' }, 
        { Name: 'firstname', Value: 'yuan' },
      ], 
      function (err, row) {
        should.exist(err);
        err.name.should.include('OTSConnectionTimeoutError');
        done();
      });
    });

    it('should return error when dns error', function (done) {
      mm.error(require('dns'), 'resolve4');
      _client.dns.domains = {
        lookup: {},
        resolve4: {}
      };
      _client.getRow('testuser', 
      [ 
        { Name: 'uid', Value: 'mk2' }, 
        { Name: 'firstname', Value: 'yuan' },
      ], 
      function (err, row) {
        should.exist(err);
        err.name.should.include('MockError');
        done();
      });
    });
  });

});
