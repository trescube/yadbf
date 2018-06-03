# YADBF (Yet Another DBF parser)

[![Greenkeeper badge](https://badges.greenkeeper.io/trescube/yadbf.svg)](https://greenkeeper.io/)
[![Build Status](https://travis-ci.org/trescube/yadbf.svg?branch=master)](https://travis-ci.org/trescube/yadbf)
[![Coverage Status](https://coveralls.io/repos/github/trescube/yadbf/badge.svg?branch=master)](https://coveralls.io/github/trescube/yadbf?branch=master)

This project is a streaming DBF parser that throws errors for the slightest transgressions.  

## Requirements

Node.js 8 or higher is required.

## Installation

Using yarn:

```bash
$ yarn add --save yadbf
```

In Node.js:

```javascript
const yadbf = require('yadbf');
const fs = require('fs');

fs.createReadStream('file.dbf')
  .pipe(yadbf())
  .on('header', header => {
    console.log(`header: ${JSON.stringify(header, null, 2)}`);
  })
  .on('data', record => {
    console.log(`record: ${JSON.stringify(record, null, 2)}`);
  })
  .on('end', () => {
    console.log('Done!');
  })
  .on('error', err => {
    console.error(`an error was thrown: ${err}`);
  });
```

## Options

The following options are available and can be passed in a single object parameter: 

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `deleted` | boolean | records flagged as deleted should be returned, non-boolean value is treated as "not supplied" | `false` |
| `offset` | integer | number of records to process before emitting | `0` |
| `size` | integer | number of records to emit | `Infinity` |

Errors are thrown if any option value type is not the supported type.  

### Notes

Deleted records do not affect operation of `offset` and `size` options.  That is, if the entire .dbf contains 2 records, deleted and not deleted, respectively, then `offset` and `size` both set to `1` would output the second record.  
