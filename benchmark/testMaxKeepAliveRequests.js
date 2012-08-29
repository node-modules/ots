/*!
 * ots - test max keep alive requests
 * http://httpd.apache.org/docs/current/mod/core.html#maxkeepaliverequests
 * 
 * Copyright(c) 2012 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var should = require('should');
var Agent = require('agentkeepalive');
var ots = require('../');
var config = require('../test/config.json');

var agent = new Agent();
var client = ots.createClient({
  accessID: config.accessID,
  accessKey: config.accessKey,
  agent: agent
});
var name = 'service.ots.aliyun.com:80';

function start() {
  // client.putData('testbenchmark', {Name: 'name', Value: 'mk2'},
  //   [
  //     {Name: 'lastname', Value: 'feng\' aerdeng' },
  //     {Name: 'nickname', Value: '  苏千\n ' }
  //   ], function (err, result) {
  //   console.log(arguments);
  // });

  var count = 0;

  function request(callback) {
    count++;
    client.getRow('testbenchmark', { Name: 'name', Value: 'mk2' }, function (err, row) {
      if (count % 10 !== 0 && count % 10 !== 1) {
        return callback();
      }
      console.log(count, row);
      console.log('%d sockets, %d socket created, %d requested',
        agent.sockets[name].length,
        agent.createSocketCount,
        agent.requestFinishedCount);
      callback();
    });
  }

  function next() {
    request(next);
  }

  next();
}

client.createTable({
  TableName: 'testbenchmark',
  PrimaryKey: [
    {'Name': 'name', 'Type': 'STRING'},
  ]
}, function (err, result) {
  console.log(arguments);
  start();
});
