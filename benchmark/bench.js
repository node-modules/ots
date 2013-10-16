/*!
 * ots - benchmark/bench.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com> (http://fengmk2.github.com)
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var utility = require('utility');
var ab = require('ab');
var Agent = require('agentkeepalive');
var ots = require('../');
var config = require('../test/config');

var method = process.argv[2];
if (!method) {
  console.log('$ node bench.js [get|put] [concurrency=5] [requests=10000] [keepalive=1]');
  process.exit(0);
}

var concurrency = parseInt(process.argv[3], 10) || 5;
var requests = parseInt(process.argv[4], 10) || 10000;
var options = {
  concurrency: concurrency,
  requests: requests
};

var keepalive = process.argv[5] === '1';

console.log('%s() benchmark with concurrency: %d, requests: %d, keepalive: %s\n', method, concurrency, requests, keepalive);

var client = ots.createClient({
  accessID: config.accessID,
  accessKey: config.accessKey,
  APIHost: config.APIHost,
  agent: keepalive ? new Agent() : null
});

var key = 'ots-bench-key';

function get() {
  var index = 0;
  ab.run(function (callback) {
    var i = index++;
    var uid = utility.md5(key + i);
    client.getRow('tcif', [{Name: 'md5', Value: uid}, {Name: 'user_id', Value: i}], 
    function (err, row) {
      callback(err, row && row.uid === uid && row.firstname === ('yuan-' + i));
    });
  }, options).on('end', function () {
    client.close();
  }).on('error', function (err) {
    console.error(err);
  });
}

function put() {
  var index = 0;
  ab.run(function (callback) {
    var i = index++;
    var uid = utility.md5(key + i);
    client.putRow('tcif', 
      [ 
        { Name: 'md5', Value: uid }, 
        { Name: 'user_id', Value: i },
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
        { Name: 'createtime', Value: new Date().toJSON() },
        { Name: 'zero', Value: 0 }
      ], 
      function (err) {
        callback(err, err ? false : true);
      }
    );
  }, options).on('end', function () {
    client.close();
  }).on('error', function (err) {
    console.error(err);
  });
}
var set = put;

client.getTableMeta('tcif', function (err, result) {
  console.log(err, result);
  client.listTable(function (err, tables) {
    if (err) {
      throw err;
    }
    eval(method + '();');
  });
});
