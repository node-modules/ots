/*!
 * ots - benchmark/parse_getrow_result.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com> (http://fengmk2.github.com)
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var fs = require('fs');
var path = require('path');
var Schema = require('protobuf-ali').Schema;
var Benchmark = require('benchmark');

var root = path.dirname(__dirname);
var schema = new Schema(fs.readFileSync(path.join(root, 'ots_protocol.desc')));
var GetRowResponse = schema['com.aliyun.cloudservice.ots.GetRowResponse'];
var GetRowRequest = schema['com.aliyun.cloudservice.ots.GetRowRequest'];

var requestBuffer = fs.readFileSync(path.join(root, 'test', 'fixtures', 'getRow_request.pb'));
var responseBuffer = fs.readFileSync(path.join(root, 'test', 'fixtures', 'getRow_result.pb'));
var responseEmptyBuffer = fs.readFileSync(path.join(root, 'test', 'fixtures', 'getRow_result_empty.pb'));

console.log('GetRowRequest %j', GetRowRequest.parse(requestBuffer));
console.log('GetRowResponse %j', GetRowResponse.parse(responseBuffer));
console.log('GetRowResponse empty %j', GetRowResponse.parse(responseEmptyBuffer));

var suite = new Benchmark.Suite();

suite
.add('GetRowRequest.parse(requestBuffer)', function () {
  GetRowRequest.parse(requestBuffer);
})
.add('GetRowResponse.parse(responseBuffer)', function () {
  GetRowResponse.parse(responseBuffer);
})
.add('GetRowResponse.parse(responseEmptyBuffer)', function () {
  GetRowResponse.parse(responseEmptyBuffer);
})

.on('cycle', function (event) {
  console.log(String(event.target));
})
.on('complete', function () {
  console.log('Fastest is ' + this.filter('fastest').pluck('name'));
})
.run();
