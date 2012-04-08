# OTS SDK

Aliyun [OTS](http://ots.aliyun.com/)(Open Table Service) SDK for [nodejs](http://nodejs.org).

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
  * PutData
  * DeleteData
  * BatchModifyData
  * GetRow
  * GetRowsByRange
  * GetRowsByOffset

## Install

```bash
$ npm install ots
```

## Usage

```javascript
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
client.putData('user', 
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
client.getRow('user', { Name: 'uid', Value: 'mk2' }, function(err, row) {
  console.log(err, row);
});
```

More examples, please see [/test/client.test.js](https://github.com/fengmk2/ots/blob/master/test/client.test.js).

## Links

* [OTS RESTful API Documents](http://ots.aliyun.com/ots_sdk/OTS_RESTful_API_2012_03_22.pdf)
* [OTS Data Model](http://ots.aliyun.com/ots_sdk/OTS_Data%20Model_2012_03_22.pdf)
* [OTS Dashboard](http://ots.aliyun.com/dashboard)
* [Python SDK](http://ots.aliyun.com/ots_sdk/ots_python_sdk_2012_03_22.zip)
* [.Net SDK](http://ots.aliyun.com/ots_sdk/Aliyun_SDK_dotNET_1_0_4458.zip)
* [Java SDK](http://storage.aliyun.com/oss/aliyun_portal_storage/oss_api/OSS_OTS_Java_SDK.zip)
* [OTS Developer Guide](http://ots.aliyun.com/guide/index)
