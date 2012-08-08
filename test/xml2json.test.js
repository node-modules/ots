/*!
 * ots - test/xml2json.test.js
 * Copyright(c) 2012 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var xml2json = require('xml2json');
var should = require('should');
var fs = require('fs');

var fixturesDir = __dirname + '/fixtures';

describe('xml2json', function () {
  it('should return string space', function () {
    // https://github.com/buglabs/node-xml2json/issues/23
    var xml = fs.readFileSync(fixturesDir + '/ots-result.xml', 'utf-8');
    var json = xml2json.toJson(xml, {object: true, space: true});
    // console.log('%j', json);

    json.should.have.property('doc');
    json.doc.Column.should.length(5);
    json.doc.Column[0].should.eql({Name: 'shit', Value: {type: 'STRING', $t: '  abc\nasdf\na '}});
    json.doc.Column[1].should.eql({Name: 'foo', Value: {type: 'STRING'}});
    json.doc.Column[2].should.eql({Name: 'foo2', Value: {type: 'STRING'}});
    json.doc.Column[3].should.eql({Name: 'bar', Value: {type: 'STRING', $t: ' '}});
    json.doc.Column[4].should.eql({PK: 'true', Name: 'uid', Value: {type: 'STRING', $t: 'god'}});
  });
});
