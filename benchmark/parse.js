/*!
 * ots - benchmark/parse.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com> (http://fengmk2.github.com)
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var Benchmark = require('benchmark');
var xml2json = require('xml2json');
var xml2jsonEDP = require('xml2json-edp');
var fs = require('fs');
var path = require('path');
var Schema = require('protobuf-ali').Schema;

var schema = new Schema(fs.readFileSync(path.join(path.dirname(__dirname), 'ots_protocol.desc')));
var Row = schema['com.aliyun.cloudservice.ots.Row'];
var suite = new Benchmark.Suite();

var pbRowBuffer = Row.serialize({
  primaryKeys: [{
    name: 'uid',
    value: {
      type: 'STRING',
      valueS: 'god'
    }
  }],
  columns: [{
    name: 'shit',
    value: {
      type: 'STRING',
      valueS: '  abc\
asdf\
a '
    }
  }, {
    name: 'foo',
    value: {
      type: 'STRING',
      valueS: ''
    }
  }, {
    name: 'foo2',
    value: {
      type: 'STRING',
      valueS: ''
    }
  }, {
    name: 'bar',
    value: {
      type: 'STRING',
      valueS: ' '
    }
  }]
});

var JSONString = JSON.stringify(Row.parse(pbRowBuffer));
// console.log('%j, json: %j', Row.parse(pbRowBuffer), JSON.parse(JSONString));

var xml = '\
<doc>\
  <Column>\
    <Name>shit</Name>\
    <Value type="STRING">  abc\
asdf\
a </Value>\
  </Column>\
  <Column><Name>foo</Name><Value type="STRING"></Value></Column>\
  <Column><Name>foo2</Name><Value type="STRING" /></Column>\
  <Column><Name>bar</Name><Value type="STRING"> </Value></Column>\
  <Column PK="true"><Name>uid</Name><Value type="STRING">god</Value></Column>\
</doc>';

suite
.add('xml2json.toJson() with {object: true, space: true}', function () {
  var json = xml2json.toJson(xml, {object: true, space: true});
})
.add('xml2json.toJson() with {object: true, space: false}', function () {
  var json = xml2json.toJson(xml, {object: true, space: false});
})
.add('xml2json.toJson() with {}', function () {
  var json = xml2json.toJson(xml);
})
.add('xml2jsonEDP.toJson() with {object: true, space: true}', function () {
  var json = xml2jsonEDP.toJson(xml, {object: true, space: true});
})
.add('xml2jsonEDP.toJson() with {object: true, space: false}', function () {
  var json = xml2jsonEDP.toJson(xml, {object: true, space: false});
})
.add('xml2jsonEDP.toJson() with {}', function () {
  var json = xml2jsonEDP.toJson(xml);
})
.add('protobuf.parse()', function () {
  Row.parse(pbRowBuffer);
})
.add('JSON.parse()', function () {
  JSON.parse(JSONString);
})

.on('cycle', function (event) {
  console.log(String(event.target));
})
.on('complete', function () {
  console.log('Fastest is ' + this.filter('fastest').pluck('name'));
})
.run({async: true, delay: 5});

