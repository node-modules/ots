# OTS SDK [![Build Status](https://secure.travis-ci.org/fengmk2/ots.png)](http://travis-ci.org/fengmk2/ots) [![Coverage Status](https://coveralls.io/repos/fengmk2/ots/badge.png)](https://coveralls.io/r/fengmk2/ots)

Aliyun [OTS](http://ots.aliyun.com/)(Open Table Service) SDK for [nodejs](http://nodejs.org).

Using `protobuf` protocol API on `ots@0.4.0+`.

## Support API

* TableGroup
  * CreateTableGroup
  * DeleteTableGroup
  * ListTableGroup
* Table
  * CreateTable
  * DeleteTable
  * GetTableMeta
  * ListTable
* Transaction
  * StartTransaction
  * CommitTransaction
  * AbortTransaction
* DataRow
  * PutRow
  * MultiPutRow (Max 100 rows)
  * DeleteRow
  * MultiDeleteRow (Max 100 rows)
  * BatchModifyRow (working)
  * GetRow
  * MultiGetRow (Max 10 rows)
  * GetRowsByRange
  * ~~GetRowsByOffset~~ (removed)

## Install

```bash
$ npm install ots
```

## Usage

```js
var ots = require('ots');
var client = ots.createClient({
  accessID: 'your accessID',
  accessKey: 'your accessKey'
});

// create a table
client.createTable({
  TableName: 'testdemo',
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
  console.log(err, result);
});

// insert a row
client.putRow('user', 
  { Name: 'uid', Value: 'mk2' }, 
  [
    { Name: 'firstname', Value: 'yuan' },
    { Name: 'lastname', Value: 'feng\' aerdeng' },
    { Name: 'nickname', Value: '苏千' },
    { Name: 'age', Value: 28 },
    { Name: 'price', Value: 110.5 },
    { Name: 'enable', Value: true },
    { Name: 'man', Value: true },
    { Name: 'female', Value: false },
    { Name: 'createtime', Value: new Date().toJSON() },
  ], function(err, result) {
  console.log(err, result);
});

// get a row
client.getRow('user', { Name: 'uid', Value: 'mk2' }, function (err, row) {
  console.log(err, row);
});
```

More examples, please see [test/client.test.js](https://github.com/fengmk2/ots/blob/master/test/client.test.js).

## Links

* [OTS RESTful API Documents](http://ots.aliyun.com/ots_sdk/OTS_RESTful_API_2012_03_22.pdf)
* [OTS Data Model](http://ots.aliyun.com/ots_sdk/OTS_Data%20Model_2012_03_22.pdf)
* [OTS Dashboard](http://ots.aliyun.com/dashboard)
* [Python SDK](http://ots.aliyun.com/ots_sdk/ots_python_sdk_2012_03_22.zip)
* [.Net SDK](http://ots.aliyun.com/ots_sdk/Aliyun_SDK_dotNET_1_0_4458.zip)
* [Java SDK](http://storage.aliyun.com/oss/aliyun_portal_storage/oss_api/OSS_OTS_Java_SDK.zip)
* [OTS Developer Guide](http://ots.aliyun.com/guide/index)

## Authors

```bash
$ git summary 

 project  : ots
 repo age : 1 year, 6 months
 active   : 17 days
 commits  : 38
 files    : 17
 authors  : 
    33  fengmk2                 86.8%
     3  tangyao                 7.9%
     2  coolme200               5.3%
```

## License 

(The MIT License)

Copyright (c) 2012 - 2013 fengmk2 &lt;fengmk2@gmail.com&gt;

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
'Software'), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
