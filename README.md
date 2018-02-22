# YADBF

[![Greenkeeper badge](https://badges.greenkeeper.io/trescube/yadbf.svg)](https://greenkeeper.io/)
[![Build Status](https://travis-ci.org/trescube/yadbf.svg?branch=master)](https://travis-ci.org/trescube/yadbf)

This project is a streaming DBF parser that throws errors for the slightest transgressions.  

## Requirements

Node.js 6 or higher is required.

## Installation

Using npm:

```bash
$ npm i --save yadbf
```

In Node.js:

```javascript
const yadbf = require('yadbf');
const fs = require('fs');

yadbf(fs.createReadStream('file.dbf'))
  .on('error', err => {
    console.error(`an error was thrown: ${err}`);
  })
  .on('header', header => {
    console.log(`header: ${JSON.stringify(header, null, 2)}`);
  })
  .on('record', record => {
    console.log(`record: ${JSON.stringify(record, null, 2)}`);
  })
  .on('end', () => {
    console.log'Done!');
  });

```

## Options

While the second parameter is accepted, it is not currently used and should be considered reserved for the glorious future.  
