# OTS SDK

Aliyun [OTS](http://ots.aliyun.com/)(Open Table Service) SDK for [nodejs](http://nodejs.org).

## Support API

* CreateTableGroup (comming soon)
* ListTableGroup (comming soon)
* CreateTable
* ListTable
* GetTableMeta
* DeleteTable
* GetRow
* GetRowsByRange (comming soon)
* GetRowsByOffset (comming soon)
* PutData
* DeleteData
* BatchModifyData (comming soon)

## Install

```bash
$ npm install ots
```

## Usage

```javascript
var ots = require('ots');
var client = ots.createClient({
  AccessID: 'your AccessID',
  AccessKey: 'your AccessKey'
});

// create a table
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
    { Name: 'createtime', Value: now.toJSON() },
  ], function(err, result) {
  console.log(err, row);
});

// get a row
client.getRow('user', { Name: 'uid', Value: 'mk2' }, function(err, row) {
  console.log(err, row);
});
```
