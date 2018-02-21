const assert = require('assert');
const yadbf = require('..');
const Duplex = require('stream').Duplex;
const fs = require('fs');

const fieldDescriptorArrayTerminator = Buffer.from([0x0D]);
const endOfFile = Buffer.from([0x1A]);

describe('header parsing', () => {
  describe('insufficient header bytes', () => {
    it('no header should emit error', done => {
      const readableStream = new Duplex();
      readableStream.push(null);

      yadbf(readableStream)
        .on('error', err => {
          assert.equal(err, 'Unable to parse first 32 bytes from header, found 0 byte(s)');
          done();
        })
        .on('header', assert.fail.bind(null, 'no header event should have been emitted'))
        .on('record', assert.fail.bind(null, 'no record events should have been emitted'));

    });

    it('insufficient header bytes should emit error', done => {
      // there should be at least 32 header bytes
      const header = Buffer.alloc(31);

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(null);

      yadbf(readableStream)
        .on('error', err => {
          assert.equal(err, 'Unable to parse first 32 bytes from header, found 31 byte(s)');
          done();
        })
        .on('header', assert.fail.bind(null, 'no header event should have been emitted'))
        .on('record', assert.fail.bind(null, 'no record events should have been emitted'));

    });

  });

  describe('version tests', () => {
    it('supported versions should not emit error', done => {
      const header = Buffer.alloc(32);
      // set a supported version number
      header.writeUInt8(0x03, 0);
      // year/month/day
      header.writeUInt8(97, 1);
      header.writeUInt8(6, 2);
      header.writeUInt8(25, 3);
      // # of records, # of header bytes, # of bytes per record
      header.writeUInt32LE(0, 4);
      header.writeUInt16LE(32+1, 8);
      header.writeUInt16LE(17, 10);
      // encryption flag
      header.writeUInt8(0x00, 15);
      // has production MDX file
      header.writeUInt8(0x01, 28);
      // language driver id/name
      header.writeUInt8(17, 29);

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(endOfFile);
      readableStream.push(null);

      yadbf(readableStream)
        .on('error', assert.fail.bind(null, 'no error event should have been emitted'))
        .on('header', actualHeader => {
          assert.equal(actualHeader.version, 3);
        })
        .on('record', assert.fail.bind(null, 'no record events should have been emitted'))
        .on('end', done);

    });

    it('unsupported versions should emit error', done => {
      const header = Buffer.alloc(32);
      // set an unsupported version number
      header.writeUInt8(0x02, 0);
      // set encryption flag
      header.writeUInt8(0, 15);

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(null);

      yadbf(readableStream)
        .on('error', err => {
          assert.equal(err, 'Unsupported version: 2');
          done();
        })
        .on('header', assert.fail.bind(null, 'no header event should have been emitted'))
        .on('record', assert.fail.bind(null, 'no record events should have been emitted'));

    });

  });

  describe('values for encrypted flag', () => {
    it('encrypted flag set to 0x01 in header should emit error', done => {
      const header = Buffer.alloc(32);
      // set valid version
      header.writeUInt8(0x03, 0);
      // set valid number of header bytes
      header.writeInt16LE(33, 8);
      // set encryption value
      header.writeUInt8(0x01, 15);

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(null);

      yadbf(readableStream)
        .on('error', err => {
          assert.equal(err, 'Encryption flag is set, cannot process');
          done();
        })
        .on('header', assert.fail.bind(null, 'no header event should have been emitted'))
        .on('record', assert.fail.bind(null, 'no record events should have been emitted'));

    });

    it('encrypted flag set to 0x00 in header should not emit error', done => {
      const header = Buffer.alloc(32);
      // set valid version
      header.writeUInt8(0x03, 0);
      // set valid number of header bytes
      header.writeInt16LE(33, 8);
      // set unencrypted value
      header.writeUInt8(0x00, 15);

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(null);

      yadbf(readableStream)
        .on('error', assert.fail.bind(null, 'no error event should have been emitted'))
        .on('header', actualHeader => {
          done();
        })
        .on('record', assert.fail.bind(null, 'no record events should have been emitted'));

    });

    it('non-0x00/0x01 encryption flag should emit error', done => {
      const header = Buffer.alloc(32);
      // set valid version
      header.writeUInt8(0x03, 0);
      // set valid number of header bytes
      header.writeInt16LE(33, 8);
      // set invalid encryption value
      header.writeUInt8(0x02, 15);

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(null);

      yadbf(readableStream)
        .on('error', err => {
          assert.equal(err, 'Invalid encryption flag value: 2');
          done();
        })
        .on('header', assert.fail.bind(null, 'no header event should have been emitted'))
        .on('record', assert.fail.bind(null, 'no record events should have been emitted'));

    });

  });

  describe('values for number of header bytes', () => {
    it('header bytes set to 33 should result in empty fields', done => {
      const header = Buffer.alloc(32);
      // valid version
      header.writeUInt8(0x8B, 0);
      // year/month/day
      header.writeUInt8(97, 1);
      header.writeUInt8(6, 2);
      header.writeUInt8(25, 3);
      // # of records, # of header bytes, # of bytes per record
      header.writeUInt32LE(0, 4);
      header.writeUInt16LE(32+1, 8);
      header.writeUInt16LE(17, 10);
      // encryption flag
      header.writeUInt8(0x00, 15);
      // has production MDX file
      header.writeUInt8(0x01, 28);
      // language driver id/name
      header.writeUInt8(17, 29);

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(endOfFile);
      readableStream.push(null);

      yadbf(readableStream)
        .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
        .on('header', actualHeader => {
          assert.deepEqual(actualHeader.fields, []);
        })
        .on('record', assert.fail.bind(null, 'no record events should have been emitted'))
        .on('end', () => {
          done();
        });

    });

    it('header bytes not set to value that is not divisble by 32 (plus 1) should emit error', done => {
      const header = Buffer.alloc(32);
      // set valid version
      header.writeUInt8(0x8B, 0);
      // set invalid number of header bytes
      header.writeUInt16LE(32+2, 8);
      // set unencrypted value
      header.writeUInt8(0x00, 15);

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(endOfFile);
      readableStream.push(null);

      yadbf(readableStream)
        .on('error', err => {
          assert.equal(err, 'Invalid number of header bytes: 34');
          done();
        })
        .on('header', assert.fail.bind(null, 'no header event should have been emitted'))
        .on('record', assert.fail.bind(null, 'no record events should have been emitted'));

    });

  });

  it('last byte of header not equal to 0x0D should emit error', done => {
    const header = Buffer.alloc(32);
    // set valid version
    header.writeUInt8(0x8B, 0);
    // set invalid number of header bytes
    header.writeUInt16LE(33, 8);
    // set unencrypted value
    header.writeUInt8(0x00, 15);

    const readableStream = new Duplex();
    readableStream.push(header);
    readableStream.push(Buffer.from([0x0C]));
    readableStream.push(endOfFile);
    readableStream.push(null);

    yadbf(readableStream)
      .on('error', err => {
        assert.equal(err, 'Invalid field descriptor array terminator at byte 33');
        done();
      })
      .on('header', assert.fail.bind(null, 'no header event should have been emitted'))
      .on('record', assert.fail.bind(null, 'no record events should have been emitted'));

  });

  describe('values for production MDX file existence', () => {
    it('value set to 0x00 in header should not emit error', done => {
      const header = Buffer.alloc(32);
      // set valid version
      header.writeUInt8(0x03, 0);
      // year/month/day
      header.writeUInt8(97, 1);
      header.writeUInt8(6, 2);
      header.writeUInt8(25, 3);
      // # of records, # of header bytes, # of bytes per record
      header.writeUInt32LE(0, 4);
      header.writeUInt16LE(32+1, 8);
      header.writeUInt16LE(17, 10);
      // encryption flag
      header.writeUInt8(0x00, 15);
      // has production MDX file
      header.writeUInt8(0x00, 28);
      // language driver id/name
      header.writeUInt8(17, 29);

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(endOfFile);
      readableStream.push(null);

      yadbf(readableStream)
        .on('error', assert.fail.bind(null, 'no error event should have been emitted'))
        .on('header', actualHeader => {
          assert.ok(!actualHeader.hasProductionMDXFile);
        })
        .on('record', assert.fail.bind(null, 'no record events should have been emitted'))
        .on('end', done);

    });

    it('value set to 0x01 in header should not emit error', done => {
      const header = Buffer.alloc(32);
      // set valid version
      header.writeUInt8(0x03, 0);
      // year/month/day
      header.writeUInt8(97, 1);
      header.writeUInt8(6, 2);
      header.writeUInt8(25, 3);
      // # of records, # of header bytes, # of bytes per record
      header.writeUInt32LE(0, 4);
      header.writeUInt16LE(32+1, 8);
      header.writeUInt16LE(17, 10);
      // encryption flag
      header.writeUInt8(0x00, 15);
      // has production MDX file
      header.writeUInt8(0x01, 28);
      // language driver id/name
      header.writeUInt8(17, 29);

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(endOfFile);
      readableStream.push(null);

      yadbf(readableStream)
        .on('error', assert.fail.bind(null, 'no error event should have been emitted'))
        .on('header', actualHeader => {
          assert.ok(actualHeader.hasProductionMDXFile);
        })
        .on('record', assert.fail.bind(null, 'no record events should have been emitted'))
        .on('end', done);

    });

    it('non-0x00/0x01 value should emit error', done => {
      const header = Buffer.alloc(32);
      // set valid version
      header.writeUInt8(0x03, 0);
      // set valid number of header bytes
      header.writeInt16LE(33, 8);
      // set valid encryption value
      header.writeUInt8(0x00, 15);
      // set invalid production MDX file existence value
      header.writeUInt8(0x02, 28);

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(null);

      yadbf(readableStream)
        .on('error', err => {
          assert.equal(err, 'Invalid production MDX file existence value: 2');
          done();
        })
        .on('header', assert.fail.bind(null, 'no header event should have been emitted'))
        .on('record', assert.fail.bind(null, 'no record events should have been emitted'));

    });

  });

  describe('field tests', () => {
    it('field length equal to 255 should emit error', done => {
      const header = Buffer.alloc(32);
      // set valid version
      header.writeUInt8(0x8B, 0);
      // set valid number of header bytes to accommodate a single field
      header.writeUInt16LE(32+32+1, 8);
      // encryption flag
      header.writeUInt8(0x00, 15);
      // has production MDX file
      header.writeUInt8(0x00, 28);

      // field definition
      const field = Buffer.alloc(32);
      field.write('field1', 0, 'field1'.length); // name
      field.write('C', 11); // type
      field.writeUInt8(255, 16); // length
      field.writeUInt8(1, 17); // decimal count
      field.writeUInt16LE(1, 18); // work area id
      field.writeUInt8(0x00, 31); // prod MDX field flag, ERROR CONDITION: must be either 0x00 or 0x01

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(field);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(null);

      yadbf(readableStream)
        .on('error', err => {
          assert.equal(err, 'Field length must be less than 255');
          done();
        })
        .on('header', assert.fail.bind(null, 'no header events should have been emitted'))
        .on('record', assert.fail.bind(null, 'no record events should have been emitted'));

    });

    it('field type not one of C, D, F, L, M, or N should emit error', done => {
      const header = Buffer.alloc(32);
      // set valid version
      header.writeUInt8(0x8B, 0);
      // set valid number of header bytes to accommodate a single field
      header.writeUInt16LE(32+32+1, 8);
      // encryption flag
      header.writeUInt8(0x00, 15);
      // has production MDX file
      header.writeUInt8(0x00, 28);

      // field definition
      const field = Buffer.alloc(32);
      field.write('field1', 0, 'field1'.length); // name
      field.write('X', 11); // type
      field.writeUInt8(17, 16); // length
      field.writeUInt8(1, 17); // decimal count
      field.writeUInt16LE(1, 18); // work area id
      field.writeUInt8(0x00, 31); // prod MDX field flag, ERROR CONDITION: must be either 0x00 or 0x01

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(field);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(null);

      yadbf(readableStream)
        .on('error', err => {
          assert.equal(err, 'Field type must be one of: C, D, F, L, M, N');
          done();
        })
        .on('header', assert.fail.bind(null, 'no header events should have been emitted'))
        .on('record', assert.fail.bind(null, 'no record events should have been emitted'));

    });

    it('non-0x00/0x01 value for production MDX file index tag should emit error', done => {
      const header = Buffer.alloc(32);
      // set valid version
      header.writeUInt8(0x8B, 0);
      // set valid number of header bytes to accommodate a single field
      header.writeUInt16LE(32+32+1, 8);
      // encryption flag
      header.writeUInt8(0x00, 15);
      // has production MDX file
      header.writeUInt8(0x00, 28);

      // field definition
      const field = Buffer.alloc(32);
      field.write('field1', 0, 'field1'.length); // name
      field.write('C', 11); // type
      field.writeUInt8(17, 16); // length
      field.writeUInt8(1, 17); // decimal count
      field.writeUInt16LE(1, 18); // work area id
      field.writeUInt8(0x02, 31); // prod MDX field flag

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(field);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(null);

      yadbf(readableStream)
        .on('error', err => {
          assert.equal(err, 'Invalid indexed in production MDX file value: 2');
          done();
        })
        .on('header', assert.fail.bind(null, 'no header events should have been emitted'))
        .on('record', assert.fail.bind(null, 'no record events should have been emitted'));

    });

    it('field with type D and not 8 bytes in length should emit error', done => {
      const header = Buffer.alloc(32);
      // set valid version
      header.writeUInt8(0x8B, 0);
      // set valid number of header bytes to accommodate a single field
      header.writeUInt16LE(32+32+1, 8);
      // encryption flag
      header.writeUInt8(0x00, 15);
      // has production MDX file
      header.writeUInt8(0x00, 28);

      // field definition
      const field = Buffer.alloc(32);
      field.write('field1', 0, 'field1'.length); // name
      field.write('D', 11); // type
      field.writeUInt8(9, 16); // length
      field.writeUInt8(1, 17); // decimal count
      field.writeUInt16LE(1, 18); // work area id
      field.writeUInt8(0x00, 31); // prod MDX field flag

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(field);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(null);

      yadbf(readableStream)
        .on('error', err => {
          assert.equal(err, 'Invalid D (date) field length: 9');
          done();
        })
        .on('header', assert.fail.bind(null, 'no header events should have been emitted'))
        .on('record', assert.fail.bind(null, 'no record events should have been emitted'));

    });

    it('field with type L and not 1 byte in length should emit error', done => {
      const header = Buffer.alloc(32);
      // set valid version
      header.writeUInt8(0x8B, 0);
      // set valid number of header bytes to accommodate a single field
      header.writeUInt16LE(32+32+1, 8);
      // encryption flag
      header.writeUInt8(0x00, 15);
      // has production MDX file
      header.writeUInt8(0x00, 28);

      // field definition
      const field = Buffer.alloc(32);
      field.write('field1', 0, 'field1'.length); // name
      field.write('L', 11); // type
      field.writeUInt8(2, 16); // length
      field.writeUInt8(1, 17); // decimal count
      field.writeUInt16LE(1, 18); // work area id
      field.writeUInt8(0x00, 31); // prod MDX field flag

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(field);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(null);

      yadbf(readableStream)
        .on('error', err => {
          assert.equal(err, 'Invalid L (logical) field length: 2');
          done();
        })
        .on('header', assert.fail.bind(null, 'no header events should have been emitted'))
        .on('record', assert.fail.bind(null, 'no record events should have been emitted'));

    });

    it('field with type M and not 10 bytes in length should emit error', done => {
      const header = Buffer.alloc(32);
      // set valid version
      header.writeUInt8(0x8B, 0);
      // set valid number of header bytes to accommodate a single field
      header.writeUInt16LE(32+32+1, 8);
      // encryption flag
      header.writeUInt8(0x00, 15);
      // has production MDX file
      header.writeUInt8(0x00, 28);

      // field definition
      const field = Buffer.alloc(32);
      field.write('field1', 0, 'field1'.length); // name
      field.write('M', 11); // type
      field.writeUInt8(11, 16); // length
      field.writeUInt8(1, 17); // decimal count
      field.writeUInt16LE(1, 18); // work area id
      field.writeUInt8(0x00, 31); // prod MDX field flag

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(field);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(null);

      yadbf(readableStream)
        .on('error', err => {
          assert.equal(err, 'Invalid M (memo) field length: 11');
          done();
        })
        .on('header', assert.fail.bind(null, 'no header events should have been emitted'))
        .on('record', assert.fail.bind(null, 'no record events should have been emitted'));

    });

  });

  describe('successful parsing', () => {
    it('all fields should be parsed', (done) => {
      const header = Buffer.alloc(32);
      // valid version
      header.writeUInt8(0x8B, 0);
      // year/month/day
      header.writeUInt8(97, 1);
      header.writeUInt8(6, 2);
      header.writeUInt8(25, 3);
      // # of records, # of header bytes, # of bytes per record
      header.writeUInt32LE(0, 4);
      header.writeUInt16LE(32+32+32+1, 8);
      header.writeUInt16LE(37, 10);
      // encryption flag
      header.writeUInt8(0x00, 15);
      // has production MDX file
      header.writeUInt8(0x01, 28);
      // language driver id/name
      header.writeUInt8(17, 29);

      // first field definition
      const field1 = Buffer.alloc(32);
      field1.write('field1', 0, 'field1'.length);
      field1.write('C', 11);
      field1.writeUInt8(157, 16); // length
      field1.writeUInt8(104, 17); // precision
      field1.writeUInt16LE(119, 18); // work area id
      field1.writeUInt8(1, 31); // prod MDX field flag

      // second field definition
      const field2 = Buffer.alloc(32);
      field2.write('field2', 0, 'field2'.length);
      field2.write('C', 11);
      field2.writeUInt8(13, 16); // length
      field2.writeUInt8(12, 17); // precision
      field2.writeUInt16LE(120, 18); // work area id
      field2.writeUInt8(0, 31); // prod MDX field flag

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(field1);
      readableStream.push(field2);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(endOfFile);
      readableStream.push(null);

      yadbf(readableStream)
        .on('error', err => {
          assert.fail(`no error should have been thrown: ${err}`);
        })
        .on('header', actualHeader => {
          assert.deepEqual(actualHeader, {
            version: 139,
            dateOfLastUpdate: new Date(1997, 5, 25),
            numberOfRecords: 0,
            numberOfHeaderBytes: 97,
            numberOfBytesInRecord: 37,
            hasProductionMDXFile: 0x01,
            langaugeDriverId: 17,
            fields: [
              {
                name: 'field1',
                type: 'C',
                length: 157,
                precision: 104,
                workAreaId: 119,
                isIndexedInMDXFile: true
              },
              {
                name: 'field2',
                type: 'C',
                length: 13,
                precision: 12,
                workAreaId: 120,
                isIndexedInMDXFile: false
              }
            ]
          });
        })
        .on('record', assert.fail.bind(null, 'no record events should have been emitted'))
        .on('end', done);

    });

  });

});

describe('record parsing', () => {
  describe('fields', () => {
    it('all non-deleted fields and records should be parsed', done => {
      const header = Buffer.alloc(32);
      // valid version
      header.writeUInt8(0x8B, 0);
      // # of records, # of header bytes
      header.writeUInt32LE(3, 4);
      header.writeUInt16LE(32+32+32+1, 8);
      // # of bytes per record: 1 byte deleted flag, 25 bytes 1st field, 30 bytes 2nd field
      header.writeUInt16LE(1+25+30, 10);
      // encryption flag
      header.writeUInt8(0x00, 15);
      // has production MDX file
      header.writeUInt8(0x01, 28);

      // first field definition
      const field1 = Buffer.alloc(32);
      field1.write('field1', 0, 'field1'.length);
      field1.write('C', 11);
      field1.writeUInt8(25, 16); // length
      field1.writeUInt8(104, 17); // precision
      field1.writeUInt16LE(119, 18); // work area id
      field1.writeUInt8(1, 31); // prod MDX field flag

      // second field definition
      const field2 = Buffer.alloc(32);
      field2.write('field2', 0, 'field2'.length);
      field2.write('C', 11);
      field2.writeUInt8(30, 16); // length
      field2.writeUInt8(12, 17); // precision
      field2.writeUInt16LE(120, 18); // work area id
      field2.writeUInt8(0, 31); // prod MDX field flag

      // first record, # of bytes per record in length
      const record1 = Buffer.alloc(1+25+30);
      record1.write(' ', 0, 1);
      record1.write('record 1 field 1 value', 1+0, 'record 1 field 1 value'.length);
      record1.write('record 1 field 2 value', 1+25, 'record 1 field 2 value'.length);

      // second record, is deleted, # of bytes per record in length
      const record2 = Buffer.alloc(1+25+30);
      record2.write('*', 0, 1);
      record2.write('record 2 field 1 value', 1+0, 'record 2 field 1 value'.length);
      record2.write('record 2 field 2 value', 1+25, 'record 2 field 2 value'.length);

      // third record, # of bytes per record in length
      const record3 = Buffer.alloc(1+25+30);
      record3.write(' ', 0, 1);
      record3.write('record 3 field 1 value', 1+0, 'record 3 field 1 value'.length);
      record3.write('record 3 field 2 value', 1+25, 'record 3 field 2 value'.length);

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(field1);
      readableStream.push(field2);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(record1);
      readableStream.push(record2);
      readableStream.push(record3);
      readableStream.push(endOfFile);
      readableStream.push(null);

      const records = [];

      yadbf(readableStream)
        .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
        .on('record', record => {
          records.push(record);
        })
        .on('end', () => {
          assert.deepEqual(records, [
            {
              '@meta': {
                deleted: false
              },
              field1: 'record 1 field 1 value',
              field2: 'record 1 field 2 value'
            },
            {
              '@meta': {
                deleted: false
              },
              field1: 'record 3 field 1 value',
              field2: 'record 3 field 2 value'
            }
          ]);

          done();
        });

    });

    it('an error should be omitted when record first byte is not a space or asterisk', done => {
      const header = Buffer.alloc(32);
      // valid version
      header.writeUInt8(0x8B, 0);
      // # of records, # of header bytes
      header.writeUInt32LE(2, 4);
      header.writeUInt16LE(32+32+1, 8);
      // # of bytes per record: 1 byte deleted flag, 25 bytes 1st field
      header.writeUInt16LE(1+25, 10);
      // encryption flag
      header.writeUInt8(0x00, 15);
      // has production MDX file
      header.writeUInt8(0x01, 28);

      // first field definition
      const field1 = Buffer.alloc(32);
      field1.write('field1', 0, 'field1'.length);
      field1.write('C', 11);
      field1.writeUInt8(25, 16); // length
      field1.writeUInt8(104, 17); // precision
      field1.writeUInt16LE(119, 18); // work area id
      field1.writeUInt8(1, 31); // prod MDX field flag

      // first record, # of bytes per record in length
      const record1 = Buffer.alloc(1+25+30);
      record1.write('#', 0, 1);
      record1.write('record 1 field 1 value', 1+0, 'record 1 field 1 value'.length);

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(field1);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(record1);
      readableStream.push(Buffer.from([0x0A]));
      readableStream.push(null);

      const records = [];

      yadbf(readableStream)
        .on('error', err => {
          assert.equal(err, 'Invalid deleted record value: #');
          done();
        })
        .on('record', assert.fail.bind(null, 'no record events should have been emitted'));

    });

    it('an error should be omitted when file does not end with 0x1A', done => {
      const header = Buffer.alloc(32);
      // valid version
      header.writeUInt8(0x8B, 0);
      // # of records, # of header bytes
      header.writeUInt32LE(1, 4);
      header.writeUInt16LE(32+32+1, 8);
      // # of bytes per record: 1 byte deleted flag, 25 bytes 1st field
      header.writeUInt16LE(1+25, 10);
      // encryption flag
      header.writeUInt8(0x00, 15);
      // has production MDX file
      header.writeUInt8(0x01, 28);

      // first field definition
      const field1 = Buffer.alloc(32);
      field1.write('field1', 0, 'field1'.length);
      field1.write('C', 11);
      field1.writeUInt8(25, 16); // length
      field1.writeUInt8(104, 17); // precision
      field1.writeUInt16LE(119, 18); // work area id
      field1.writeUInt8(1, 31); // prod MDX field flag

      // first record, # of bytes per record in length
      const record1 = Buffer.alloc(1+25);
      record1.write(' ', 0, 1);
      record1.write('record 1 field 1 value', 1+0, 'record 1 field 1 value'.length);

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(field1);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(record1);
      readableStream.push(Buffer.from('Z'));
      readableStream.push(null);

      yadbf(readableStream)
        .on('error', err => {
          assert.equal(err, 'Last byte of file is not end-of-file marker');
          done();
        });

    });

    it('an error should be omitted when last character is not 0x1A', done => {
      const header = Buffer.alloc(32);
      // valid version
      header.writeUInt8(0x8B, 0);
      // # of records, # of header bytes
      header.writeUInt32LE(1, 4);
      header.writeUInt16LE(32+32+1, 8);
      // # of bytes per record: 1 byte deleted flag, 25 bytes 1st field
      header.writeUInt16LE(1+25, 10);
      // encryption flag
      header.writeUInt8(0x00, 15);
      // has production MDX file
      header.writeUInt8(0x01, 28);

      // first field definition
      const field1 = Buffer.alloc(32);
      field1.write('field1', 0, 'field1'.length);
      field1.write('C', 11);
      field1.writeUInt8(25, 16); // length
      field1.writeUInt8(104, 17); // precision
      field1.writeUInt16LE(119, 18); // work area id
      field1.writeUInt8(1, 31); // prod MDX field flag

      // first record, # of bytes per record in length
      const record1 = Buffer.alloc(1+25);
      record1.write(' ', 0, 1);
      record1.write('record 1 field 1 value', 1+0, 'record 1 field 1 value'.length);

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(field1);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(record1);
      readableStream.push(Buffer.from([0x1B]));
      readableStream.push(null);

      yadbf(readableStream)
        .on('error', err => {
          assert.equal(err, 'Last byte of file is not end-of-file marker');
          done();
        });

    });

  });

  describe('C-type field parsing', () => {
    it('values should right-trimmed but not left-trimmed', done => {
      const header = Buffer.alloc(32);
      // valid version
      header.writeUInt8(0x8B, 0);
      // # of records, # of header bytes
      header.writeUInt32LE(1, 4);
      header.writeUInt16LE(32+32+1, 8);
      // # of bytes per record: 1 byte deleted flag, 10 bytes for character field
      header.writeUInt16LE(1+10, 10);
      // encryption flag
      header.writeUInt8(0x00, 15);
      // has production MDX file
      header.writeUInt8(0x01, 28);

      // first field definition
      const field1 = Buffer.alloc(32);
      field1.write('C_field', 0, 'C_field'.length);
      field1.write('C', 11);
      field1.writeUInt8(10, 16); // length
      field1.writeUInt8(1, 31); // prod MDX field flag

      // first record, # of bytes per record in length
      const record1 = Buffer.alloc(1+10);
      record1.write(' ', 0);
      record1.write('  value   ', 1);

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(field1);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(record1);
      readableStream.push(Buffer.from([0x0A]));
      readableStream.push(null);

      yadbf(readableStream)
        .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
        .on('record', record => {
          assert.equal(record.C_field, '  value');
          done();
        });

    });

  });

  describe('D-type field parsing', () => {
    it('D-type fields should be parsed as dates', done => {
      const header = Buffer.alloc(32);
      // valid version
      header.writeUInt8(0x8B, 0);
      // # of records, # of header bytes
      header.writeUInt32LE(1, 4);
      header.writeUInt16LE(32+32+1, 8);
      // # of bytes per record: 1 byte deleted flag, 8 bytes for 'D' field
      header.writeUInt16LE(1+8, 10);
      // encryption flag
      header.writeUInt8(0x00, 15);
      // has production MDX file
      header.writeUInt8(0x01, 28);

      // first field definition
      const field1 = Buffer.alloc(32);
      field1.write('field1', 0, 'field1'.length);
      field1.write('D', 11);
      field1.writeUInt8(8, 16); // length
      field1.writeUInt8(104, 17); // precision
      field1.writeUInt16LE(119, 18); // work area id
      field1.writeUInt8(1, 31); // prod MDX field flag

      // first record, # of bytes per record in length
      const record1 = Buffer.alloc(1+8);
      record1.write(' ', 0, 1);
      record1.write('19520719', 1, 8);

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(field1);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(record1);
      readableStream.push(Buffer.from([0x1A]));
      readableStream.push(null);

      const records = [];

      yadbf(readableStream)
        .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
        .on('record', record => {
          records.push(record);
        })
        .on('end', () => {
          assert.deepEqual(records, [
            {
              '@meta': {
                deleted: false
              },
              field1: new Date(1952, 7, 19)
            }
          ]);

          done();
        });

    });

  });

  describe('L-type field parsing', () => {
    ['Y', 'y', 'T', 't'].forEach(truthy_value => {
      it(`L-type fields should treat '${truthy_value}' as true`, done => {
        const header = Buffer.alloc(32);
        // valid version
        header.writeUInt8(0x8B, 0);
        // # of records, # of header bytes
        header.writeUInt32LE(1, 4);
        header.writeUInt16LE(32+32+1, 8);
        // # of bytes per record: 1 byte deleted flag, 1 byte for logical field
        header.writeUInt16LE(1+1, 10);
        // encryption flag
        header.writeUInt8(0x00, 15);
        // has production MDX file
        header.writeUInt8(0x01, 28);

        // first field definition
        const field1 = Buffer.alloc(32);
        field1.write('L_field', 0, 'L_field'.length);
        field1.write('L', 11);
        field1.writeUInt8(1, 16); // length
        field1.writeUInt8(1, 31); // prod MDX field flag

        // first record, # of bytes per record in length
        const record1 = Buffer.alloc(1+1);
        record1.write(' ', 0);
        record1.write(truthy_value, 1);

        const readableStream = new Duplex();
        readableStream.push(header);
        readableStream.push(field1);
        readableStream.push(fieldDescriptorArrayTerminator);
        readableStream.push(record1);
        readableStream.push(Buffer.from([0x0A]));
        readableStream.push(null);

        yadbf(readableStream)
          .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
          .on('record', record => {
            assert.equal(record.L_field, true);
            done();
          });

      });

    });

    ['N', 'n', 'F', 'f'].forEach(falsey_value => {
      it(`L-type fields should treat '${falsey_value}' as false`, done => {
        const header = Buffer.alloc(32);
        // valid version
        header.writeUInt8(0x8B, 0);
        // # of records, # of header bytes
        header.writeUInt32LE(1, 4);
        header.writeUInt16LE(32+32+1, 8);
        // # of bytes per record: 1 byte deleted flag, 1 byte for logical field
        header.writeUInt16LE(1+1, 10);
        // encryption flag
        header.writeUInt8(0x00, 15);
        // has production MDX file
        header.writeUInt8(0x01, 28);

        // first field definition
        const field1 = Buffer.alloc(32);
        field1.write('L_field', 0, 'L_field'.length);
        field1.write('L', 11);
        field1.writeUInt8(1, 16); // length
        field1.writeUInt8(1, 31); // prod MDX field flag

        // first record, # of bytes per record in length
        const record1 = Buffer.alloc(1+1);
        record1.write(' ', 0);
        record1.write(falsey_value, 1);

        const readableStream = new Duplex();
        readableStream.push(header);
        readableStream.push(field1);
        readableStream.push(fieldDescriptorArrayTerminator);
        readableStream.push(record1);
        readableStream.push(Buffer.from([0x0A]));
        readableStream.push(null);

        yadbf(readableStream)
          .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
          .on('record', record => {
            assert.equal(record.L_field, false);
            done();
          });

      });

    });

    it('L-type fields should treat \'?\' as undefined', done => {
      const header = Buffer.alloc(32);
      // valid version
      header.writeUInt8(0x8B, 0);
      // # of records, # of header bytes
      header.writeUInt32LE(1, 4);
      header.writeUInt16LE(32+32+1, 8);
      // # of bytes per record: 1 byte deleted flag, 1 byte for logical field
      header.writeUInt16LE(1+1, 10);
      // encryption flag
      header.writeUInt8(0x00, 15);
      // has production MDX file
      header.writeUInt8(0x01, 28);

      // first field definition
      const field1 = Buffer.alloc(32);
      field1.write('L_field', 0, 'L_field'.length);
      field1.write('L', 11);
      field1.writeUInt8(1, 16); // length
      field1.writeUInt8(1, 31); // prod MDX field flag

      // first record, # of bytes per record in length
      const record1 = Buffer.alloc(1+1);
      record1.write(' ', 0);
      record1.write('?', 1);

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(field1);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(record1);
      readableStream.push(Buffer.from([0x0A]));
      readableStream.push(null);

      yadbf(readableStream)
        .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
        .on('record', record => {
          assert.equal(record.L_field, undefined);
          done();
        });

    });

    it('L-type fields should emit error on unknown fields', done => {
      const header = Buffer.alloc(32);
      // valid version
      header.writeUInt8(0x8B, 0);
      // # of records, # of header bytes
      header.writeUInt32LE(1, 4);
      header.writeUInt16LE(32+32+1, 8);
      // # of bytes per record: 1 byte deleted flag, 1 byte for logical field
      header.writeUInt16LE(1+1, 10);
      // encryption flag
      header.writeUInt8(0x00, 15);
      // has production MDX file
      header.writeUInt8(0x01, 28);

      // first field definition
      const field1 = Buffer.alloc(32);
      field1.write('L_field', 0, 'L_field'.length);
      field1.write('L', 11);
      field1.writeUInt8(1, 16); // length
      field1.writeUInt8(1, 31); // prod MDX field flag

      // first record, # of bytes per record in length
      const record1 = Buffer.alloc(1+1);
      record1.write(' ', 0);
      record1.write('R', 1);

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(field1);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(record1);
      readableStream.push(Buffer.from([0x0A]));
      readableStream.push(null);

      yadbf(readableStream)
        .on('error', err => {
          assert.equal(err, 'Invalid L-type field value: R');
          done();
        })
        .on('record', assert.fail.bind(null, 'no record events should have been emitted'));

    });

  });

  describe('F/n-type field parsing', () => {
    it('F-type field', done => {
      const header = Buffer.alloc(32);
      // valid version
      header.writeUInt8(0x8B, 0);
      // # of records, # of header bytes
      header.writeUInt32LE(1, 4);
      header.writeUInt16LE(32+32+1, 8);
      // # of bytes per record: 1 byte for deleted flag, 19 byte for logical field
      header.writeUInt16LE(1+19, 10);
      // encryption flag
      header.writeUInt8(0x00, 15);
      // has production MDX file
      header.writeUInt8(0x01, 28);

      // first field definition
      const field1 = Buffer.alloc(32);
      field1.write('F_field', 0, 'L_field'.length);
      field1.write('F', 11);
      field1.writeUInt8(19, 16); // length
      field1.writeUInt8(11, 17); // precision
      field1.writeUInt8(1, 31); // prod MDX field flag

      // first record, # of bytes per record in length
      const record1 = Buffer.alloc(1+19);
      record1.write(' ', 0);
      record1.write('123.45678', 1, '123.45678'.length);

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(field1);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(record1);
      readableStream.push(Buffer.from([0x0A]));
      readableStream.push(null);

      yadbf(readableStream)
        .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
        .on('record', record => {
          assert.equal(record.F_field, 123.45678);
          done();
        });

    });

    it('N-type field', done => {
      const header = Buffer.alloc(32);
      // valid version
      header.writeUInt8(0x8B, 0);
      // # of records, # of header bytes
      header.writeUInt32LE(1, 4);
      header.writeUInt16LE(32+32+1, 8);
      // # of bytes per record: 1 byte for deleted flag, 19 byte for logical field
      header.writeUInt16LE(1+19, 10);
      // encryption flag
      header.writeUInt8(0x00, 15);
      // has production MDX file
      header.writeUInt8(0x01, 28);

      // first field definition
      const field1 = Buffer.alloc(32);
      field1.write('N_field', 0, 'N_field'.length);
      field1.write('N', 11);
      field1.writeUInt8(19, 16); // length
      field1.writeUInt8(11, 17); // precision
      field1.writeUInt8(1, 31); // prod MDX field flag

      // first record, # of bytes per record in length
      const record1 = Buffer.alloc(1+19);
      record1.write(' ', 0);
      record1.write('123.45678', 1, '123.45678'.length);

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(field1);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(record1);
      readableStream.push(Buffer.from([0x0A]));
      readableStream.push(null);

      yadbf(readableStream)
        .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
        .on('record', record => {
          assert.equal(record.N_field, 123.45678);
          done();
        });

    });

  });

  describe('M-type field parsing', () => {
    it('field values consisting of all numbers should be accepted', done => {
      const header = Buffer.alloc(32);
      // valid version
      header.writeUInt8(0x8B, 0);
      // # of records, # of header bytes
      header.writeUInt32LE(1, 4);
      header.writeUInt16LE(32+32+1, 8);
      // # of bytes per record: 1 byte deleted flag, 10 bytes for 'M' field
      header.writeUInt16LE(1+10, 10);
      // encryption flag
      header.writeUInt8(0x00, 15);
      // has production MDX file
      header.writeUInt8(0x01, 28);

      // first field definition
      const field1 = Buffer.alloc(32);
      field1.write('field1', 0, 'field1'.length);
      field1.write('M', 11);
      field1.writeUInt8(10, 16); // length
      field1.writeUInt16LE(119, 18); // work area id
      field1.writeUInt8(1, 31); // prod MDX field flag

      // first record, # of bytes per record in length
      const record1 = Buffer.alloc(1+10);
      record1.write(' ', 0, 1);
      record1.write('1357924680', 1, 10);

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(field1);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(record1);
      readableStream.push(endOfFile);
      readableStream.push(null);

      const records = [];

      yadbf(readableStream)
        .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
        .on('record', record => {
          records.push(record);
        })
        .on('end', () => {
          assert.deepEqual(records, [
            {
              '@meta': {
                deleted: false
              },
              field1: '1357924680'
            }
          ]);

          done();
        });

    });

    it('field values consisting of all spaces should be accepted', done => {
      const header = Buffer.alloc(32);
      // valid version
      header.writeUInt8(0x8B, 0);
      // # of records, # of header bytes
      header.writeUInt32LE(1, 4);
      header.writeUInt16LE(32+32+1, 8);
      // # of bytes per record: 1 byte deleted flag, 10 bytes for 'M' field
      header.writeUInt16LE(1+10, 10);
      // encryption flag
      header.writeUInt8(0x00, 15);
      // has production MDX file
      header.writeUInt8(0x01, 28);

      // first field definition
      const field1 = Buffer.alloc(32);
      field1.write('field1', 0, 'field1'.length);
      field1.write('M', 11);
      field1.writeUInt8(10, 16); // length
      field1.writeUInt16LE(119, 18); // work area id
      field1.writeUInt8(1, 31); // prod MDX field flag

      // first record, # of bytes per record in length
      const record1 = Buffer.alloc(1+10);
      record1.write(' ', 0, 1);
      record1.write('          ', 1, 10);

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(field1);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(record1);
      readableStream.push(endOfFile);
      readableStream.push(null);

      const records = [];

      yadbf(readableStream)
        .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
        .on('record', record => {
          records.push(record);
        })
        .on('end', () => {
          assert.deepEqual(records, [
            {
              '@meta': {
                deleted: false
              },
              field1: '          '
            }
          ]);

          done();
        });

    });

    it('field values not entirely number or spaces should emit error', done => {
      const header = Buffer.alloc(32);
      // valid version
      header.writeUInt8(0x8B, 0);
      // # of records, # of header bytes
      header.writeUInt32LE(1, 4);
      header.writeUInt16LE(32+32+1, 8);
      // # of bytes per record: 1 byte deleted flag, 10 bytes for 'M' field
      header.writeUInt16LE(1+10, 10);
      // encryption flag
      header.writeUInt8(0x00, 15);
      // has production MDX file
      header.writeUInt8(0x01, 28);

      // first field definition
      const field1 = Buffer.alloc(32);
      field1.write('field1', 0, 'field1'.length);
      field1.write('M', 11);
      field1.writeUInt8(10, 16); // length
      field1.writeUInt16LE(119, 18); // work area id
      field1.writeUInt8(1, 31); // prod MDX field flag

      // first record, # of bytes per record in length
      const record1 = Buffer.alloc(1+10);
      record1.write(' ', 0, 1);
      record1.write('     4    ', 1, 10);

      const readableStream = new Duplex();
      readableStream.push(header);
      readableStream.push(field1);
      readableStream.push(fieldDescriptorArrayTerminator);
      readableStream.push(record1);
      readableStream.push(endOfFile);
      readableStream.push(null);

      yadbf(readableStream)
        .on('error', err => {
          assert.equal(err, 'Invalid M-type field value: \'     4    \'');
          done();
        })
        .on('record', assert.fail.bind(null, 'no record events should have been emitted'));

    });

  });

});
