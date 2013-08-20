/*!
 * ots - test/client.test.js
 * Copyright(c) 2012 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

/**
 * Module dependencies.
 */

var ots = require('../');
var should = require('should');
var config = require('./config.json');
var EventProxy = require('eventproxy').EventProxy;
var crypto = require('crypto');
var mm = require('mm');

function md5(s) {
  var hash = crypto.createHash('md5');
  hash.update(s);
  return hash.digest('hex');
}

describe('client.test.js', function() {
  var client = ots.createClient({
    accessID: config.accessID,
    accessKey: config.accessKey,
    APIHost: config.APIHost
  });

  afterEach(mm.restore);

  before(function (done) {
    var ep = EventProxy.create('testgroup', 'test', 'testuser', 'testurl', function () {
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
      PagingKeyLen: 1,
    }, function (err, result) {
      ep.emit('testuser')
    });
    client.createTable({
      TableName: 'testurl',
      PrimaryKey: [
        { 'Name': 'md5', 'Type': 'STRING' },
      ],
      PagingKeyLen: 0,
    }, function (err, result) {
      ep.emit('testurl')
    });
  });

  it('should sign by sorted', function () {
    var params = [
      [ 'TableName', 'CapTable' ],
      [ 'PK.1.Name', 'PrimaryKey1' ],
      [ 'PK.1.Type', 'STRING' ],
      [ 'PK.2.Name', 'PrimaryKey2' ],
      [ 'PK.2.Type', 'INTEGER' ],
      [ 'View.1.Name', 'View1' ],
      [ 'View.1.PK.1.Name', 'PrimaryKey1' ],
      [ 'View.1.PK.1.Type', 'STRING' ],
      [ 'View.1.PK.2.Name', 'Column1' ],
      [ 'View.1.PK.2.Type', 'BOOLEAN' ],
      [ 'View.1.Column.1.Name', 'Column2' ],
      [ 'View.1.Column.1.Type', 'STRING' ],
      [ 'View.1.Column.2.Name', 'Column3' ],
      [ 'View.1.Column.2.Type', 'DOUBLE' ],
    ];
    var params = client.signature('/CreateTable', params);
    params.join('&').should.include('&Signature=');
  });

  describe('createTableGroup()', function () {
    it('should create a group', function (done) {
      client.createTableGroup('testgroup', 'STRING', function (err, result) {
        should.not.exist(err);
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
        groups.should.include('testgroup');
        done();
      });
    });
  });

  describe('deleteTableGroup()', function () {
    it('should delete a group', function (done) {
      client.deleteTableGroup('testgroup', function (err, result) {
        should.not.exist(err);
        client.deleteTableGroup('testgroup', function (err, result) {
          should.exist(err);
          err.name.should.equal('OTSStorageObjectNotExist');
          done();
        });
      });
    });
  });

  describe('createTable()', function () {

    it('should return OTSMissingParameter error', function (done) {
      client.createTable({ table: { TableName: 'test' } }, function (err, result) {
        should.exist(err);
        err.name.should.equal('OTSMissingParameter');
        err.message.should.equal('The request must contain the parameter of PK.1.Name');
        done();
      });
    });

    it('should create "test" table success', function(done) {
      client.createTable({
        TableName: 'test',
        PrimaryKey: [
          {'Name': 'uid', 'Type': 'STRING'},
        ],
        PagingKeyLen: 0,
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
           'PagingKeyLen': 2
          }
        ]
      }, function(err, result) {
        should.not.exist(err);
        should.exist(result);
        done();
      });
    });

    it('should get "test" table meta success', function (done) {
      client.getTableMeta('test', function (err, meta) {
        should.not.exist(err);
        // console.log('%j', meta)
        meta.should.have.keys([ 'TableName', 'PrimaryKey', 'PagingKeyLen', 'View' ]);
        meta.TableName.should.equal('test');
        meta.PrimaryKey.should.have.keys([ 'Name', 'Type' ]);
        meta.PagingKeyLen.should.equal('0');
        meta.View.PrimaryKey.should.length(3);
        meta.View.Column.should.length(2);
        meta.View.Name.should.equal('view1');
        done();
      });
    });

    it('should list table success', function(done) {
      client.listTable(function(err, tablenames) {
        should.not.exist(err);
        tablenames.should.be.an.instanceof(Array);
        tablenames.should.include('test');
        done();
      });
    });

    it('should create "test" table exist error', function(done) {
      client.createTable({ 
        TableName: 'test', 
        PrimaryKey: [ { Name: 'id', Type: 'STRING' } ] 
      }, function(err, result) {
        should.exist(err);
        err.name.should.equal('OTSStorageObjectAlreadyExist');
        err.message.should.equal('Requested table/view does exist.');
        done();
      });
    });

    it('should delete "test" table success and error', function(done) {
      client.deleteTable('test', function(err, result) {
        should.not.exist(err);
        should.exist(result);
        client.deleteTable('test', function(err, result) {
          should.exist(err);
          err.name.should.equal('OTSStorageObjectNotExist');
          err.message.should.equal('Requested table/view doesn\'t exist.');
          should.exist(result);
          done();
        });
      });
    });

  });

  var transactionID = null;
  describe('startTransaction()', function() {
    it('should start and get a transaction id', function(done) {
      client.startTransaction('user', 'foo', function(err, tid) {
        should.not.exist(err);
        tid.should.be.a('string');
        transactionID = tid;
        done();
      });
    });
  });

  describe('#commitTransaction()', function() {
    it('should commit a transaction', function(done) {
      client.commitTransaction(transactionID, function(err, result) {
        should.not.exist(err);
        done();
      });
    });
    it('should OTSParameterInvalid when commit a error tranID', function(done) {
      client.commitTransaction('errorTransactionID', function(err, result) {
        should.exist(err);
        err.name.should.equal('OTSParameterInvalid');
        err.message.should.equal('TransactionID is invalid.');
        done();
      });
    });
  });

  describe('abortTransaction()', function() {
    it('should abort a transaction success', function(done) {
      client.startTransaction('user', 'foo-need-to-abort', function(err, tid) {
        client.abortTransaction(tid, function(err, result) {
          should.not.exist(err);
          result.Code.should.equal('OK');
          done();
        });
      });
    });

    it('should OTSStorageSessionNotExist when abort a committed tran', function(done) {
      client.abortTransaction(transactionID, function(err, result) {
        should.exist(err);
        err.name.should.equal('OTSStorageSessionNotExist');
        done();
      });
    });

    it('should OTSParameterInvalid when abort a error tranID', function(done) {
      client.abortTransaction('errorTransactionID', function(err, result) {
        should.exist(err);
        err.name.should.equal('OTSParameterInvalid');
        err.message.should.equal('TransactionID is invalid.');
        done();
      });
    });
  });

  var now = new Date();
  describe('putData()', function () {
    it('should insert a row', function (done) {
      client.putData('testuser', 
        [ 
          { Name: 'uid', Value: 'mk2' }, 
          { Name: 'firstname', Value: 'yuan' },
        ],
        [
          { Name: 'lastname', Value: 'feng\' aerdeng' },
          { Name: 'nickname', Value: '  苏千\n ' },
          { Name: 'age', Value: 28 },
          { Name: 'json', Value: '{ "foo": "bar" }' },
          { Name: 'price', Value: 110.5 },
          { Name: 'enable', Value: true },
          { Name: 'man', Value: true },
          { Name: 'status', Value: null },
          { Name: 'female', Value: false },
          { Name: 'createtime', Value: now.toJSON() },
        ], 
      function (err, result) {
        should.not.exist(err);
        result.should.have.property('Code', 'OK');
        result.should.have.property('RequestID');
        done();
      });
    });
  });

  describe('getRow()', function () {
    
    it('should return error', function (done) {
      client.getRow('testuser', 
      [ 
        { Name: 'uid1', Value: 'mk2' }, 
        { Name: 'firstname', Value: 'yuan' },
      ], 
      function (err, row) {
        should.exist(err);
        err.name.should.include('OTSMetaNotMatch');
        err.data.should.have.property('Error');
        err.data.Error.should.have.property('RequestID');
        err.data.Error.should.have.property('HostID');
        done();
      });
    });

    it('should return a row', function (done) {
      client.getRow('testuser', 
      [ 
        { Name: 'uid', Value: 'mk2' }, 
        { Name: 'firstname', Value: 'yuan' },
      ], 
      function (err, row) {
        should.not.exist(err);
        // console.log(row);
        row.should.have.keys([ 
          'uid', 'firstname', 
          'lastname', 'nickname',
          'age', 'price', 'enable',
          'man', 'female', 
          'json', 'status',
          'createtime'
        ]);
        row.uid.should.equal('mk2');
        row.firstname.should.equal('yuan');
        row.lastname.should.equal('feng\' aerdeng');
        row.nickname.should.equal('  苏千\n ');
        row.age.should.equal(28);
        row.price.should.equal(110.5);
        row.enable.should.equal(true);
        row.man.should.equal(true);
        row.female.should.equal(false);
        row.status.should.equal('null');
        row.createtime.should.equal(now.toJSON());
        row.json.should.equal('{ "foo": "bar" }');
        done();
      });
    });

    it('should return null when pk not exists', function(done) {
      client.getRow('testuser', 
      [ 
        { Name: 'uid', Value: 'not-existskey' }, 
        { Name: 'firstname', Value: 'haha' },
      ], function(err, row) {
        should.not.exist(err);
        should.not.exist(row);
        done();
      });
    });
  });

  describe('getRowsByOffset()', function () {
    before(function (done) {
      // insert 20 users first.
      var ep = EventProxy.create();
      ep.after('putDataDone', 20, function () {
        done();
      });
      for (var i = 0; i < 20; i++) {
        client.putData('testuser', 
        [ 
          { Name: 'uid', Value: 'testuser_' + (i % 2) }, 
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
          { Name: 'createtime', Value: new Date().toJSON() },
        ], function (err, result) {
          // should.not.exist(err);
          ep.emit('putDataDone');
        });
      }
    });
    it('should get 5 users, testuser_0 offset:0 top:5', function(done) {
      client.getRowsByOffset('testuser', { Name: 'uid', Value: 'testuser_0' }, null, 0, 5, 
      function(err, rows) {
        should.not.exist(err);
        rows.should.length(5);
        for (var i = rows.length; i--; ) {
          var row = rows[i];
          row.should.have.keys([ 
            'uid', 'firstname', 
            'lastname', 'nickname',
            'age', 'price', 'enable',
            'man', 'female', 'createtime'
          ]);
        }
        done();
      });
    });
    it('should get 5 users, testuser_0 offset:5 top:5', function(done) {
      client.getRowsByOffset('testuser', { Name: 'uid', Value: 'testuser_0' }, 
      [ 'firstname', 'age', 'createtime' ], 5, 5, function(err, rows) {
        should.not.exist(err);
        rows.should.length(5);
        for (var i = rows.length; i--; ) {
          var row = rows[i];
          row.should.have.keys([ 
            'firstname', 
            'age', 'createtime'
          ]);
        }
        done();
      });
    });
    it('should get 0 users, testuser_0 offset:10 top:5', function(done) {
      client.getRowsByOffset('testuser', { Name: 'uid', Value: 'testuser_0' }, 
      [ 'age' ], 10, 5, function(err, rows) {
        should.not.exist(err);
        rows.should.length(0);
        done();
      });
    });
  });

  describe('getRowsByRange()', function() {
    before(function(done) {
      // insert 10 urls first.
      var ep = EventProxy.create();
      ep.after('putDataDone', 10, function() {
        done();
      });
      for (var i = 0; i < 10; i++) {
        var url = 'http://t.cn/abcd' + i;
        client.putData('testurl', 
        [ 
          { Name: 'md5', Value: md5(url) }, 
        ],
        [
          { Name: 'url', Value: url },
          { Name: 'createtime', Value: new Date().toJSON() },
        ], function(err, result) {
          should.not.exist(err);
          ep.emit('putDataDone');
        });
      }
    });
    var nextBegin = null;
    it('should get 6 rows, top:5', function(done) {
      client.getRowsByRange('testurl', null, 
      { Name: 'md5', Begin: ots.STR_MIN, End: ots.STR_MAX }, null, 6, 
      function(err, rows) {
        should.not.exist(err);
        rows.should.length(6);
        for (var i = rows.length; i--; ) {
          var row = rows[i];
          row.should.have.keys([ 
            'md5', 'url', 'createtime'
          ]);
        }
        nextBegin = rows[rows.length - 1].md5;
        done();
      });
    });
    it('should get 5 rows, top:5 next', function(done) {
      client.getRowsByRange('testurl', null, 
      { Name: 'md5', Begin: nextBegin, End: ots.STR_MAX }, null, 6, 
      function(err, rows) {
        should.not.exist(err);
        rows.should.length(6);
        for (var i = rows.length; i--; ) {
          var row = rows[i];
          row.should.have.keys([ 
            'md5', 'url', 'createtime'
          ]);
        }
        nextBegin = rows[rows.length - 1].md5;
        done();
      });
    });
  });

  describe('deleteData()', function () {
    it('should delete a row', function (done) {
      client.deleteData('testuser', 
        [
          {Name: 'uid', Value: 'mk2'},
          {Name: 'firstname', Value: 'yuan'}
        ],
      function (err, result) {
        should.not.exist(err);
        result.should.have.property('Code', 'OK');
        // TODO: WTF, delete delay?!
        client.getRow('testuser', [
            {Name: 'uid', Value: 'mk2'},
            {Name: 'firstname', Value: 'yuan'}
          ],
        function (err, row) {
          should.not.exist(err);
          should.not.exist(row);
          done();
        });
      });
    });

    it('should delete by a not exists key', function (done) {
      client.deleteData('testuser', [
        {Name: 'uid', Value: 'not-existskey'},
        {Name: 'firstname', Value: 'yuan'}
      ], function (err, result) {
        should.not.exist(err);
        result.should.have.property('Code', 'OK');
        done();
      });
    });
  });

  describe('batchModifyData()', function () {
    var url = 'http://t.cn/abc' + new Date().getTime();
    var urlmd5 = md5(url);
    var transactionID = null;

    after(function(done) {
      client.abortTransaction(transactionID, function (err) {
        // console.log(arguments)
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
        ], tid, function (err, result) {
          should.not.exist(err);
          result.Code.should.equal('OK');
          client.commitTransaction(tid, function (err) {
            should.not.exist(err);
            done();
          });
        });
      });
    });
  });

  describe('mock()', function () {
    var _client = ots.createClient({
      accessID: config.accessID,
      accessKey: config.accessKey,
      APIHost: 'http://service.ots.aliyun.com:80',
      requestTimeout: 0.0001
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
        err.name.should.include('OTSRequestTimeoutError');
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