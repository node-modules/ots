/*!
 * ots - test/client.test.js
 * 
 * Copyright(c) 2012 - 2013 fengmk2 <fengmk2@gmail.com> (http://fengmk2.github.com/)
 * MIT Licensed
 */

/**
 * Module dependencies.
 */

var should = require('should');
var utility = require('utility');
var urllib = require('urllib');
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
    var ep = EventProxy.create('testgroup', 'test', 'testuser', 'testuser_bigint', 'testurl', 
      'testuser_range', 
      'testuser_relation',
    function () {
      done();
    });
    client.deleteTableGroup('node_ots_client_testgroup', function (err) {
      ep.emit('testgroup');
    });
    client.deleteTable('node_ots_client_test', function (err) {
      ep.emit('test');
    });
    client.createTable({
      TableName: 'node_ots_client_testuser',
      PrimaryKey: [
        { 'Name': 'uid', 'Type': 'STRING' },
        { 'Name': 'firstname', 'Type': 'STRING' },
      ],
    }, function (err, result) {
      ep.emit('testuser')
    });

    client.createTable({
      TableName: 'node_ots_client_testuser_bigint',
      PrimaryKey: [
        { 'Name': 'uid', 'Type': 'INTEGER' },
      ],
    }, function (err, result) {
      ep.emit('testuser_bigint')
    });

    client.createTable({
      TableName: 'node_ots_client_testuser_relation',
      PrimaryKey: [
        { 'Name': 'uid', 'Type': 'STRING' },
        { 'Name': 'sid', 'Type': 'STRING' },
      ],
    }, function (err, result) {
      ep.emit('testuser_relation')
    });

    client.createTable({
      TableName: 'node_ots_client_testuser_range',
      PrimaryKey: [
        { Name: 'uid_md5', Type: 'STRING' },
        { Name: 'uid', Type: 'STRING' },
        { Name: 'create_time', Type: 'STRING' }
      ]
    }, function (err, result) {
      ep.emit('testuser_range');
    });

    client.createTable({
      TableName: 'node_ots_client_testurl',
      PrimaryKey: [
        { 'Name': 'md5', 'Type': 'STRING' },
      ],
    }, function (err, result) {
      ep.emit('testurl')
    });
  });

  describe('importData()', function () {
    it('should import data to a table', function (done) {
      var requests = {
        Schema: [ ['uid', 'S'], ['sid', 'S'] ],
        Data: [
          { PK: ['1', '1'], Column: [ ['c1', 'I', 1], ['gmt', 'S', Date()] ] },
          { PK: ['1', '1'], Column: [ ['c1', 'I', 1], ['gmt', 'S', Date()] ] },
          { PK: ['1', '1'], Column: [ ['c1', 'I', 1], ['gmt', 'S', Date()] ] },
          { PK: ['1', '1'], Column: [ ['c1', 'I', 1], ['gmt', 'S', Date()] ] },
          { PK: ['1', '1'], Column: [ ['c1', 'I', 1], ['gmt', 'S', Date()] ] },
          { PK: ['2', '1'], Column: [ ['c1', 'I', 2], ['gmt', 'S', Date()] ] },
          { PK: ['3', '1'], Column: [ ['c1', 'I', 3], ['gmt', 'S', Date()] ] },
          { PK: ['4', '2'], Column: [ ['c1', 'I', 4], ['gmt', 'S', Date()] ] },
          { PK: ['2', '2'], Column: [ ['c1', 'I', 5], ['gmt', 'S', Date()] ] },
          { PK: ['2', '3'], Column: [ ['c1', 'I', 6], ['gmt', 'S', Date()] ] },
          { PK: ['2', '4'], Column: [ ['c1', 'I', 7], ['gmt', 'S', Date()] ] },
        ]
      };
      client.importData('node_ots_client_testuser_relation', requests, function (err, result) {
        should.not.exist(err);
        var pks = [
          [ {Name: 'uid', Value: '1'}, {Name: 'sid', Value: '1'} ],
          [ {Name: 'uid', Value: '2'}, {Name: 'sid', Value: '1'} ],
          [ {Name: 'uid', Value: '3'}, {Name: 'sid', Value: '1'} ],
          [ {Name: 'uid', Value: '4'}, {Name: 'sid', Value: '2'} ],
          [ {Name: 'uid', Value: '2'}, {Name: 'sid', Value: '2'} ],
          [ {Name: 'uid', Value: '2'}, {Name: 'sid', Value: '3'} ],
          [ {Name: 'uid', Value: '2'}, {Name: 'sid', Value: '4'} ],
        ];
        client.multiGetRow('node_ots_client_testuser_relation', pks, function (err, datas) {
          should.not.exist(err);
          datas.should.length(pks.length);
          datas.forEach(function (data, i) {
            data.should.have.keys('isSucceed', 'error', 'tableName', 'row');
            data.row.should.have.keys('c1', 'gmt', 'uid', 'sid');
            data.row.c1.should.be.a.Number;
            data.row.c1.should.equal(i + 1);
          });
          done();
        });
      });
    });

    it('should import empty rows to a table', function (done) {
      var requests = {
        Schema: [ ['uid', 'S'], ['sid', 'S'] ],
        Data: [
        ]
      };
      client.importData('node_ots_client_testuser_relation', requests, function (err, result) {
        should.not.exist(err);
        result.Code.should.equal('OK');
        done();
      });
    });

    it('should import wrong', function (done) {
      var requests = {
        Schema: [ ['uid', 'S'], ['sid', 'I'] ],
        Data: [
        ]
      };
      client.importData('node_ots_client_testuser_relation', requests, function (err, result) {
        should.exist(err);
        err.name.should.equal('OTSMetaNotMatch');
        err.message.should.equal('Primary key meta defined in the request does not match with the Table meta.');
        done();
      });
    });

    it('should mock error', function (done) {
      var requests = {
        Schema: [ ['uid', 'S'], ['sid', 'S'] ],
        Data: [
        ]
      };
      mm.error(urllib, 'request');
      client.importData('node_ots_client_testuser_relation', requests, function (err, result) {
        should.exist(err);
        err.name.should.equal('OTSMockError');
        should.not.exist(result);
        done();
      });
    });
  });

  describe('createTableGroup()', function () {
    it('should create a group success', function (done) {
      client.createTableGroup('node_ots_client_testgroup', 'STRING', function (err) {
        should.not.exist(err);
        client.createTableGroup('node_ots_client_testgroup', 'STRING', function (err) {
          should.exist(err);
          err.name.should.equal('OTSStorageObjectAlreadyExistError');
          err.message.should.equal('Requested table/view does exist.');
          err.code.should.equal('OTSStorageObjectAlreadyExist');
          done();
        });
      });
    });

    it('should create a group with wrong key type', function (done) {
      client.createTableGroup('node_ots_client_testgroup', 'BOOLEAN', function (err) {
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

    it('should return ErrorMessage.parse Malformed message OTSMalformedErrorMessageError', function (done) {
      mm.data(urllib, 'request', new Buffer('wrong format'));
      client.listTableGroup(function (err, groups) {
        should.exist(err);
        err.name.should.equal('OTSMalformedErrorMessageError');
        err.message.should.equal('Malformed message');
        should.not.exist(groups);
        done();
      });
    });

    it('should return pbResponse.parse Malformed message OTSMalformedListTableGroupMessageError', function (done) {
      mm(urllib, 'request', function (url, options, callback) {
        process.nextTick(function () {
          callback(null, new Buffer('wrong response format'), {statusCode: 200});
        });
      });
      client.listTableGroup(function (err, groups) {
        should.exist(err);
        err.name.should.equal('OTSMalformedListTableGroupMessageError');
        err.message.should.equal('Malformed message');
        should.not.exist(groups);
        done();
      });
    });
  });

  describe('deleteTableGroup()', function () {
    before(function (done) {
      client.createTableGroup('node_ots_client_testgroup', 'STRING', function (err) {
        done();
      });
    });

    it('should delete a group', function (done) {
      client.deleteTableGroup('node_ots_client_testgroup', function (err) {
        should.not.exist(err);
        client.deleteTableGroup('node_ots_client_testgroup', function (err) {
          should.exist(err);
          err.name.should.equal('OTSStorageObjectNotExistError');
          err.message.should.equal('Requested table/view doesn\'t exist.');
          console.log(err)
          err.url.should.include('&requestid=');
          err.url.should.include('&hostid=');
          done();
        });
      });
    });
  });

  describe('createTable()', function () {

    it('should return OTSParameterInvalidError when missing primary key', function (done) {
      client.createTable({ TableName: 'node_ots_client_test' }, function (err, result) {
        should.exist(err);
        err.name.should.equal('OTSParameterInvalidError');
        err.message.should.equal('The Table/View does not specify the primary key.');
        done();
      });
    });

    it('should create "node_ots_client_test" table success', function (done) {
      client.createTable({
        TableName: 'node_ots_client_test',
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

    it('should create "node_ots_client_testuser_bigint" table success', function (done) {
      done = pedding(5, done);

      var rowkey = [
        {Name: 'uid', Type: 'INTEGER', Value: '2523370015105311489'},
      ];
      client.putRow('node_ots_client_testuser_bigint', rowkey, [{Name: 'name', Value: 'suqian'}], 
      function (err, result) {
        should.not.exist(err);
        client.getRow('node_ots_client_testuser_bigint', rowkey, function (err, data) {
          should.not.exist(err);
          data.should.eql({ name: 'suqian', uid: '2523370015105311489' });
          data.uid.should.be.a.String;
          data.uid.should.equal('2523370015105311489');
          done();
        });
      });

      var rowkey4 = [
        {Name: 'uid', Value: 2523370015105311489},
      ];
      // should got wrong value when number to long
      client.putRow('node_ots_client_testuser_bigint', rowkey4, [{Name: 'name', Value: 'suqian'}], 
      function (err, result) {
        should.not.exist(err);
        client.getRow('node_ots_client_testuser_bigint', rowkey4, function (err, data) {
          should.not.exist(err);
          data.uid.should.not.equal('2523370015105311489');
          done();
        });
      });

      var rowkey2 = [
        {Name: 'uid', Type: 'INTEGER', Value: Math.pow(2, 53)},
      ];
      client.putRow('node_ots_client_testuser_bigint', rowkey2, [{Name: 'name', Value: 'suqian'}], 
      function (err, result) {
        should.not.exist(err);
        client.getRow('node_ots_client_testuser_bigint', rowkey2, function (err, data) {
          should.not.exist(err);
          data.should.eql({ name: 'suqian', uid: Math.pow(2, 53) });
          data.uid.should.be.a.String;
          data.uid.should.equal(String(Math.pow(2, 53)));
          done();
        });
      });

      var rowkey3 = [
        {Name: 'uid', Type: 'INTEGER', Value: 100},
      ];
      client.putRow('node_ots_client_testuser_bigint', rowkey3, [{Name: 'name', Value: 'suqian'}], 
      function (err, result) {
        should.not.exist(err);
        client.getRow('node_ots_client_testuser_bigint', rowkey3, function (err, data) {
          should.not.exist(err);
          data.should.eql({ name: 'suqian', uid: 100 });
          data.uid.should.be.a.Number;
          data.uid.should.equal(100);
          done();
        });
      });

      var rowkey5 = [
        {Name: 'uid', Value: 0},
      ];
      client.putRow('node_ots_client_testuser_bigint', rowkey5, [{Name: 'name', Value: 'suqian'}], 
      function (err, result) {
        should.not.exist(err);
        client.getRow('node_ots_client_testuser_bigint', rowkey5, function (err, data) {
          should.not.exist(err);
          data.should.eql({ name: 'suqian', uid: 0 });
          data.uid.should.be.a.Number;
          data.uid.should.equal(0);
          done();
        });
      });
    });

    it('should get "node_ots_client_test" table meta success', function (done) {
      client.getTableMeta('node_ots_client_test', function (err, meta) {
        should.not.exist(err);
        // console.log('%j', meta)
        meta.should.have.keys([ 'tableName', 'primaryKeys', 'views' ]);
        meta.tableName.should.equal('node_ots_client_test');
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
        tablenames.should.include('node_ots_client_test');
        done();
      });
    });

    it('should create "node_ots_client_test" table exist error', function (done) {
      client.createTable({ 
        TableName: 'node_ots_client_test', 
        PrimaryKey: [ { Name: 'id', Type: 'STRING' } ] 
      }, function (err, result) {
        should.exist(err);
        err.name.should.equal('OTSStorageObjectAlreadyExistError');
        err.message.should.equal('Requested table/view does exist.');
        done();
      });
    });

    it('should delete "node_ots_client_test" table success and error', function (done) {
      client.deleteTable('node_ots_client_test', function (err) {
        should.not.exist(err);
        client.deleteTable('node_ots_client_test', function (err) {
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
    after(function (done) {
      if (transactionID) {
        client.abortTransaction(transactionID, function (err) {
          done();
        });
      } else {
        done();
      }
    });

    describe('startTransaction()', function () {
      it('should start and get a transaction id', function (done) {
        client.startTransaction('node_ots_client_testuser', 'foo1', function (err, tid) {
          should.not.exist(err);
          should.exist(tid);
          tid.length.should.be.above(32);
          tid.should.be.a.String;
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
        client.startTransaction('node_ots_client_testuser', 'foo-need-to-abort', function (err, tid) {
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
  var MAX_SAFE_INT = Math.pow(2, 53) - 1;
  var MIN_SAFE_INT = -MAX_SAFE_INT;
  describe('putRow()', function () {
    it('should insert a row success', function (done) {
      client.putRow('node_ots_client_testuser', 
        [ 
          { Name: 'uid', Value: 'mk2' }, 
          { Name: 'firstname', Value: 'yuan' },
        ],
        [
          { Name: 'lastname', Value: 'feng\' mk2' },
          { Name: 'nickname', Value: '  苏千\n ', Type: 'STRING' },
          { Name: 'age', Value: 28 }, // int64
          { Name: 'json', Value: '{ "foo": "bar" }' },
          { Name: 'price', Value: 110, Type: 'DOUBLE' },
          { Name: 'enable', Value: true },
          { Name: 'man', Value: true },
          { Name: 'status', Value: null },
          { Name: 'female', Value: false, Type: 'BOOLEAN' },
          { Name: 'createtime', Value: now.toJSON() },
          { Name: 'zero', Value: 0, Type: 'INTEGER' },
          { Name: 'maxSafeInt', Value: MAX_SAFE_INT },
          { Name: 'minSafeInt', Value: MIN_SAFE_INT },
          { Name: 'unSafeInt1', Value: '1' + MAX_SAFE_INT },
          { Name: 'unSafeInt2', Value: 1 + MAX_SAFE_INT },
          { Name: 'unSafeInt3', Value: 3 + MAX_SAFE_INT },
          { Name: 'unSafeInt4', Value: MIN_SAFE_INT - 1 },
          { Name: 'unSafeInt5', Value: MIN_SAFE_INT - 2 },
          { Name: 'unSafeInt6', Value: MIN_SAFE_INT - 3 },
          { Name: 'unSafeInt7', Value: MIN_SAFE_INT + '123' },
        ], 
      function (err) {
        should.not.exist(err);
        done();
      });
    });

    it('should UPDATE a row error when pk not exists', function (done) {
      client.putRow('node_ots_client_testuser', 
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
      client.putRow('node_ots_client_testuser', 
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
      client.putData('node_ots_client_testuser', 
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

      client.getRow('node_ots_client_testuser', 
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
      client.getRow('node_ots_client_testuser', 
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
      client.getRow('node_ots_client_testuser', 
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
          'haha',
          'maxSafeInt', 'minSafeInt', 'zero',
          'unSafeInt1', 'unSafeInt2', 'unSafeInt3', 'unSafeInt4',
          'unSafeInt5', 'unSafeInt6', 'unSafeInt7',
        ]);
        row.uid.should.equal('mk2');
        row.firstname.should.equal('yuan');
        row.lastname.should.equal('feng\' mk2');
        row.nickname.should.equal('  苏千\n ');
        row.age.should.equal(28);
        row.price.should.equal(110.5);
        row.enable.should.equal(true);
        row.man.should.equal(true);
        row.female.should.equal(false);
        row.status.should.equal('null');
        row.createtime.should.equal(now.toJSON());
        row.json.should.equal('{ "foo": "bar" }');
        row.haha.should.equal('哈哈');

        row.zero.should.equal(0);
        row.maxSafeInt.should.equal(9007199254740991);
        row.minSafeInt.should.equal(-9007199254740991);
        row.unSafeInt1.should.equal('19007199254740991');
        row.unSafeInt2.should.equal('9007199254740992');
        row.unSafeInt3.should.equal('9007199254740994');
        row.unSafeInt4.should.equal('-9007199254740992');
        row.unSafeInt5.should.equal('-9007199254740992');
        row.unSafeInt6.should.equal('-9007199254740994');
        row.unSafeInt7.should.equal('-9007199254740991123');
        done();
      });
    });

    it('should return a row some columns', function (done) {
      client.getRow('node_ots_client_testuser', 
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

      client.getRow('node_ots_client_testuser', 
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

      client.getRow('node_ots_client_testuser', 
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

      client.getRow('node_ots_client_testuser', 
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

      client.getRow('node_ots_client_testuser', 
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

      client.getRow('node_ots_client_testuser', 
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

      client.getRow('node_ots_client_testuser', 
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
      client.getRow('node_ots_client_testuser', 
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
        client.getRow('node_ots_client_testuser_range', [
          { Name: 'uid_md5', Value: 'md51' }, 
          { Name: 'uid', Value: '320' },
          { Name: 'create_time', Value: '2013090' },
        ], function (err, row) {
          should.not.exist(err);
          done();
        });
      });
      for (var i = 0; i < 10; i++) {
        client.putRow('node_ots_client_testuser_range', 
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
      client.getRowsByRange('node_ots_client_testuser_range', 
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
      client.getRowsByRange('node_ots_client_testuser_range', 
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
      client.getRowsByRange('node_ots_client_testuser_range', 
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
      client.deleteData('node_ots_client_testuser', 
        [
          {Name: 'uid', Value: 'mk2'},
          {Name: 'firstname', Value: 'yuan'}
        ],
      function (err) {
        should.not.exist(err);
        // TODO: WTF, delete delay?!
        client.getRow('node_ots_client_testuser', [
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
      client.deleteRow('node_ots_client_testuser', [
        {Name: 'uid', Value: 'not-existskey'},
        {Name: 'firstname', Value: 'yuan'}
      ], function (err) {
        should.not.exist(err);
        done();
      });
    });

    it('should delete row with wrong pk', function (done) {
      client.deleteRow('node_ots_client_testuser', [
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
      client.startTransaction('node_ots_client_testurl', urlmd5, function (err, tid) {
        should.not.exist(err);
        tid.should.be.a.String;
        transactionID = tid;
        client.batchModifyData('node_ots_client_testurl', 
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
      client.multiPutRow('node_ots_client_testuser', items, function (err, results) {
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
      client.multiPutRow('node_ots_client_testuser', items, function (err, results) {
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
      client.multiDeleteRow('node_ots_client_testuser', items, function (err, results) {
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
      client.multiDeleteRow('node_ots_client_testuser', items, function (err, results) {
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
      client.multiDeleteRow('node_ots_client_testuser', items, function (err, results) {
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
      client.multiDeleteRow('node_ots_client_testuser', items, function (err, results) {
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
        client.putData('node_ots_client_testuser', 
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
      client.multiGetRow('node_ots_client_testuser', pks, function (err, items) {
        should.not.exist(err);
        items.should.length(10);
        for (var i = 0; i < 5; i++) {
          var item = items[i];
          item.isSucceed.should.equal(true);
          item.error.should.eql({ code: 'OK' });
          item.tableName.should.equal('node_ots_client_testuser');
          item.row.should.have.keys('uid', 'age', 'createtime', 'enable', 'female', 'index', 'lastname',
            'man', 'nickname', 'price', 'firstname');
          item.row.index.should.equal(i);
        }
        for (var i = 5; i < 10; i++) {
          var item = items[i];
          item.isSucceed.should.equal(true);
          item.error.should.eql({ code: 'OK' });
          item.tableName.should.equal('node_ots_client_testuser');
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
      client.multiGetRow('node_ots_client_testuser', pks, function (err, items) {
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
      client.multiGetRow('node_ots_client_testuser', pks, function (err, items) {
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
      _client.getRow('node_ots_client_testuser', 
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
      _client.getRow('node_ots_client_testuser', 
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
