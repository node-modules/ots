var Benchmark = require('benchmark');
var xml2json = require('xml2json');
var suite = new Benchmark.Suite();

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
</doc>\
';

suite
.add('toJson() with {object: true, space: true}', function () {
  var json = xml2json.toJson(xml, {object: true, space: true});
})
.add('toJson() with {object: true, space: false}', function () {
  var json = xml2json.toJson(xml, {object: true, space: false});
})
.add('toJson() with {}', function () {
  var json = xml2json.toJson(xml);
})
// add listeners
.on('cycle', function (event, bench) {
  console.log(String(bench));
})
.on('complete', function () {
  console.log('Fastest is ' + this.filter('fastest').pluck('name'));
})
.run({async: true, delay: 5});

