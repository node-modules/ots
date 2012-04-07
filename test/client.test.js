/*!
 * ots - test/client.test.js
 * Copyright(c) 2012 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

/**
 * Module dependencies.
 */

var Client = require('../lib/client');
var should = require('should');
var config = require('./config.json')

describe('client.test.js', function() {
  var client = new Client({
    AccessID: config.AccessID,
    AccessKey: config.AccessKey,
  });

  before(function(done) {
    // ensure "test" table delete
    client.deleteTable('test', function(err) {
      if (err && err.name === 'OTSStorageObjectNotExist') {
        err = null;
      }
      done(err);
    });
  });

  it('should sign by sorted', function() {
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

  describe('#createTable()', function() {

    it('should return OTSMissingParameter error', function(done) {
      client.createTable({ table: { TableName: 'test' } }, function(err, result) {
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

    it('should get "test" table meta success', function(done) {
      client.getTableMeta('test', function(err, meta) {
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

  var now = new Date();
  describe('#putData()', function() {
    it('should insert a row', function(done) {
      client.putData('user', { Name: 'uid', Value: 'mk2' }, [
          { Name: 'firstname', Value: 'yuan' },
          { Name: 'lastname', Value: 'feng\' aerdeng' },
          { Name: 'nickname', Value: '苏千' },
          { Name: 'age', Value: 28 },
          { Name: 'price', Value: 110.5 },
          { Name: 'enable', Value: true },
          { Name: 'man', Value: true },
          { Name: 'female', Value: false },
          { Name: 'createtime', Value: now.toJSON() },
        ], function(err, result) {
        should.not.exist(err);
        done();
      });
    });
  });

  describe('#getRow()', function() {
    it('should return a row', function(done) {
      client.getRow('user', { Name: 'uid', Value: 'mk2' }, function(err, row) {
        should.not.exist(err);
        // console.log(row);
        row.should.have.keys([ 
          'uid', 'firstname', 'lastname', 'nickname',
          'age', 'price', 'enable',
          'man', 'female', 'createtime'
        ]);
        row.uid.should.equal('mk2');
        row.firstname.should.equal('yuan');
        row.lastname.should.equal('feng\' aerdeng');
        row.nickname.should.equal('苏千');
        row.age.should.equal(28);
        row.price.should.equal(110.5);
        row.enable.should.equal(true);
        row.man.should.equal(true);
        row.female.should.equal(false);
        row.createtime.should.equal(now.toJSON());
        new Date(row.createtime).should.eql(now);
        done();
      });
    });

    it('should return null when pk not exists', function(done) {
      client.getRow('user', { Name: 'uid', Value: 'not-existskey' }, function(err, row) {
        should.not.exist(err);
        should.not.exist(row);
        done();
      });
    });
  });

  describe('#deleteData()', function() {
    it('should delete a row', function(done) {
      client.deleteData('user', { Name: 'uid', Value: 'mk2' }, function(err, result) {
        should.not.exist(err);
        client.getRow('user', { Name: 'uid', Value: 'mk2' }, function(err, row) {
          should.not.exist(err);
          should.not.exist(row);
          done();
        });
      });
    });

    it('should delete by a not exists key', function(done) {
      client.deleteData('user', { Name: 'uid', Value: 'not-existskey' }, function(err, result) {
        should.not.exist(err);
        done();
      });
    });
  });

});