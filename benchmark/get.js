/*!
 * ots - benchmark/get.js
 * Copyright(c) 2013 fengmk2 <fengmk2@gmail.com> (http://fengmk2.github.com)
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */

var ots = require('../');
var config = require('../test/config');

var client = ots.createClient({
  accessID: config.accessID,
  accessKey: config.accessKey,
  APIHost: config.APIHost
});

client.getRow('testuser', 
  [ 
    { Name: 'uid', Value: 'mk2not' }, 
    { Name: 'firstname', Value: 'yuanexists' },
  ], 
  function (err, row) {
    console.log(arguments);
  }
);