const assert = require('assert');
const YADBF = require('..');
const { Readable } = require('stream');

class DBF {
  constructor (build) {
    this.build = build;
  }

  get buffer() {
    const header = Buffer.alloc(32);
    header.writeUInt8(this.build._version, 0);

    header.writeUInt8(this.build._date.getYear(), 1);
    header.writeUInt8(this.build._date.getMonth(), 2);
    header.writeUInt8(this.build._date.getDate(), 3);

    if (this.build._numberOfRecords) {
      header.writeUInt32LE(this.build._numberOfRecords, 4);
    } else {
      header.writeUInt32LE(this.build._records.length, 4);
    }

    if (this.build._numberOfHeaderBytes) {
      header.writeUInt16LE(this.build._numberOfHeaderBytes, 8);
    } else {
      header.writeUInt16LE(32 + (this.build._fields.length * 32) + 1, 8);
    }

    if (this.build._numberOfBytesPerRecord) {
      header.writeUInt16LE(this.build._numberOfBytesPerRecord, 10);
    } else {
      // when not explicitly specified, calculate bytes-per-record from fields + 1 (deleted flag)
      const numberOfBytesPerRecord = this.build._fields.reduce((byteCount, field) => {
        return byteCount + field.size;
      }, 1);

      header.writeUInt16LE(numberOfBytesPerRecord, 10);
    }

    header.writeUInt8(this.build._encrypted, 15);
    header.writeUInt8(this.build._hasProductionMDXFile, 28);
    header.writeUInt8(this.build._languageDriverId, 29);

    return Buffer.concat([
      header,
      ...this.build._fields.map(field => field.buffer),
      Buffer.from([this.build._fieldDescriptorArrayTerminator]),
      ...this.build._records.map(record => record.buffer),
      Buffer.from([this.build._endOfFile])
    ]);
  }

  static get Builder() {
    class Builder {
      constructor() {
        this._version = 0x8B;
        this._date = new Date();
        this._fields = [];
        this._records = [];
        this._encrypted = false;
        this._fieldDescriptorArrayTerminator = 0x0D;
        this._endOfFile = 0x1A;
      }

      version(_version) {
        this._version = _version;
        return this;
      }

      date(_date) {
        this._date = _date;
        return this;
      }

      numberOfRecords(_numberOfRecords) {
        this._numberOfRecords = _numberOfRecords;
        return this;
      }

      numberOfHeaderBytes(_numberOfHeaderBytes) {
        this._numberOfHeaderBytes = _numberOfHeaderBytes;
        return this;
      }

      numberOfBytesPerRecord(_numberOfBytesPerRecord) {
        this._numberOfBytesPerRecord = _numberOfBytesPerRecord;
        return this;
      }

      field(_field) {
        this._fields.push(_field);
        return this;
      }

      record(_record) {
        this._records.push(_record);
        return this;
      }

      encrypted(_encrypted = true) {
        this._encrypted = _encrypted;
        return this;
      }

      hasProductionMDXFile(_hasProductionMDXFile) {
        this._hasProductionMDXFile = _hasProductionMDXFile;
        return this;
      }

      languageDriverId(_languageDriverId) {
        this._languageDriverId = _languageDriverId;
        return this;
      }

      fieldDescriptorArrayTerminator(_fieldDescriptorArrayTerminator) {
        this._fieldDescriptorArrayTerminator = _fieldDescriptorArrayTerminator;
        return this;
      }

      endOfFile(_endOfFile) {
        this._endOfFile = _endOfFile;
        return this;
      }

      build() {
        return new DBF(this);
      }
    }

    return Builder;
  }
}

class Field {
  constructor(build) {
    this.build = build;
  }

  get size() {
    return this.build._size;
  }

  get buffer() {
    const buffer = Buffer.alloc(32);
    buffer.write(this.build._name, 0, this.build._name.length);
    buffer.write(this.build._type, 11);
    buffer.writeUInt8(this.build._size, 16);
    if (this.build._precision) {
      buffer.writeUInt8(this.build._precision, 17);
    }
    buffer.writeUInt16LE(this.build._workAreaId, 18);
    buffer.writeUInt8(this.build._prodMDXFieldFlag, 31);
    return buffer;
  }

  static get Builder() {
    class Builder {
      constructor(_name, _type) {
        this._name = _name;
        this._type = _type;

        switch (_type) {
          case 'D':
            this._size = 8; break;
          case 'L':
            this._size = 1; break;
          case 'M':
            this._size = 10; break;
          default:
            this._size = 0; break;
        }

        this._precision = 0;
        this._workAreaId = 0;
        this._prodMDXFieldFlag = false;
      }

      size(_size) {
        this._size = _size;
        return this;
      }

      precision(_precision) {
        this._precision = _precision;
        return this;
      }

      workAreaId(_workAreaId) {
        this._workAreaId = _workAreaId;
        return this;
      }

      prodMDXFieldFlag(_prodMDXFieldFlag = true) {
        this._prodMDXFieldFlag = _prodMDXFieldFlag;
        return this;
      }

      build() {
        return new Field(this);
      }
    }

    return Builder;
  }
}

class Record {
  constructor(build) {
    this.build = build;
  }

  get buffer() {
    const totalLength = this.build._sizes.reduce((acc, cur) => acc+cur, 1);

    const buffer = Buffer.alloc(totalLength);
    buffer.write(this.build._deleted, 0, 1);

    this.build._values.reduce((offset, value, idx) => {
      buffer.write(value, offset, value.length);
      return offset + this.build._sizes[idx];
    }, 1);
    return buffer;
  }

  static get Builder() {
    class Builder {
      constructor() {
        this._deleted = ' ';
        this._values = [];
        this._sizes = [];
      }

      field(_value, _field) {
        this._values.push(_value);
        this._sizes.push(_field.size);
        return this;
      }

      deleted(_deleted = '*') {
        this._deleted = _deleted;
        return this;
      }

      build() {
        return new Record(this);
      }
    }

    return Builder;
  }
}

describe('header parsing', () => {
  describe('insufficient header bytes', () => {
    it('no header should emit error', done => {
      const readableStream = new Readable();
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', err => {
          assert.equal(err, 'Unable to parse first 32 bytes from header, found 0 byte(s)');
        })
        .on('header', assert.fail.bind(null, 'no header event should have been emitted'))
        .on('data', assert.fail.bind(null, 'no record events should have been emitted'))
        .on('end', done);
    });

    it('insufficient header bytes should emit error', done => {
      // there should be at least 32 header bytes
      const header = Buffer.alloc(31);

      const readableStream = new Readable();
      readableStream.push(header);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', err => {
          assert.equal(err, 'Unable to parse first 32 bytes from header, found 31 byte(s)');
        })
        .on('header', assert.fail.bind(null, 'no header event should have been emitted'))
        .on('data', assert.fail.bind(null, 'no record events should have been emitted'))
        .on('end', done);
    });
  });

  describe('version tests', () => {
    it('supported versions should not emit error', done => {
      // THIS MUST BE FIXED TO REMOVE OVERRIDE
      const dbf = new DBF.Builder().version(0x03).build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', assert.fail.bind(null, 'no error event should have been emitted'))
        .on('header', actualHeader => {
          assert.equal(actualHeader.version, 3);
        })
        .on('data', assert.fail.bind(null, 'no record events should have been emitted'))
        .on('end', done);
    });

    it('unsupported versions should emit error', done => {
      const dbf = new DBF.Builder().version(0x02).build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', err => {
          assert.equal(err, 'Error: Unsupported version: 2');
        })
        .on('header', assert.fail.bind(null, 'no header event should have been emitted'))
        .on('data', assert.fail.bind(null, 'no record events should have been emitted'))
        .on('end', done);
    });
  });

  describe('values for encrypted flag', () => {
    it('encrypted flag set to 0x01 in header should emit error', done => {
      const dbf = new DBF.Builder().encrypted().build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', err => {
          assert.equal(err, 'Error: Encryption flag is set, cannot process');
        })
        .on('header', assert.fail.bind(null, 'no header event should have been emitted'))
        .on('data', assert.fail.bind(null, 'no record events should have been emitted'))
        .on('end', done);
    });

    it('encrypted flag set to 0x00 in header should not emit error', done => {
      const dbf = new DBF.Builder().encrypted(0x00).build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', assert.fail.bind(null, 'no error event should have been emitted'))
        .on('header', actualHeader => {
          assert.ok('header should have been emitted');
        })
        .on('data', assert.fail.bind(null, 'no record events should have been emitted'))
        .on('end', done);
    });

    it('non-0x00/0x01 encryption flag should emit error', done => {
      const dbf = new DBF.Builder().encrypted(0x02).build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', err => {
          assert.equal(err, 'Error: Invalid encryption flag value: 2');
        })
        .on('header', assert.fail.bind(null, 'no header event should have been emitted'))
        .on('data', assert.fail.bind(null, 'no record events should have been emitted'))
        .on('end', done);
    });
  });

  describe('values for number of header bytes', () => {
    it('no fields described in header should result in empty fields', done => {
      const dbf = new DBF.Builder().build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
        .on('header', actualHeader => {
          assert.deepEqual(actualHeader.fields, []);
        })
        .on('data', assert.fail.bind(null, 'no record events should have been emitted'))
        .on('end', done);
    });

    it('header bytes not set to value that is not divisble by 32 (plus 1) should emit error', done => {
      const dbf = new DBF.Builder().numberOfHeaderBytes(32+2).build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', err => {
          assert.equal(err, 'Error: Invalid number of header bytes: 34');
        })
        .on('header', assert.fail.bind(null, 'no header event should have been emitted'))
        .on('data', assert.fail.bind(null, 'no record events should have been emitted'))
        .on('end', done);
    });
  });

  it('last byte of header not equal to 0x0D should emit error', done => {
    const dbf = new DBF.Builder().fieldDescriptorArrayTerminator(0x0C).build();

    const readableStream = new Readable();
    readableStream.push(dbf.buffer);
    readableStream.push(null);

    readableStream
      .pipe(new YADBF())
      .on('error', err => {
        assert.equal(err, 'Error: Invalid field descriptor array terminator at byte 33');
      })
      .on('header', assert.fail.bind(null, 'no header event should have been emitted'))
      .on('data', assert.fail.bind(null, 'no record events should have been emitted'))
      .on('end', done);
  });

  describe('values for production MDX file existence', () => {
    it('value set to 0x01 in header should not emit error', done => {
      const dbf = new DBF.Builder().hasProductionMDXFile(0x01).build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', assert.fail.bind(null, 'no error event should have been emitted'))
        .on('header', actualHeader => {
          assert.ok(actualHeader.hasProductionMDXFile);
        })
        .on('data', assert.fail.bind(null, 'no record events should have been emitted'))
        .on('end', done);
    });

    it('non-0x00/0x01 value should emit error', done => {
      const dbf = new DBF.Builder().hasProductionMDXFile(0x02).build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', err => {
          assert.equal(err, 'Error: Invalid production MDX file existence value: 2');
        })
        .on('header', assert.fail.bind(null, 'no header event should have been emitted'))
        .on('data', assert.fail.bind(null, 'no record events should have been emitted'))
        .on('end', done);
    });
  });

  describe('field tests', () => {
    it('field length equal to 255 should emit error', done => {
      const field = new Field.Builder('field1', 'C').size(255).build();

      const dbf = new DBF.Builder().field(field).build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', err => {
          assert.equal(err, 'Error: Field length must be less than 255');
        })
        .on('header', assert.fail.bind(null, 'no header events should have been emitted'))
        .on('data', assert.fail.bind(null, 'no record events should have been emitted'))
        .on('end', done);
    });

    it('field type not one of C, D, F, L, M, or N should emit error', done => {
      const field = new Field.Builder('field1', 'X').size(17).build();

      const dbf = new DBF.Builder().field(field).build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', err => {
          assert.equal(err, 'Error: Field type must be one of: C, D, F, L, M, N');
        })
        .on('header', assert.fail.bind(null, 'no header events should have been emitted'))
        .on('data', assert.fail.bind(null, 'no record events should have been emitted'))
        .on('end', done);
    });

    it('non-0x00/0x01 value for production MDX file index tag should emit error', done => {
      const field = new Field.Builder('field1', 'C').size(17).prodMDXFieldFlag(0x02).build();

      const dbf = new DBF.Builder().field(field).build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', err => {
          assert.equal(err, 'Error: Invalid indexed in production MDX file value: 2');
        })
        .on('header', assert.fail.bind(null, 'no header events should have been emitted'))
        .on('data', assert.fail.bind(null, 'no record events should have been emitted'))
        .on('end', done);
    });

    it('field with type D and not 8 bytes in length should emit error', done => {
      const field = new Field.Builder('field1', 'D').size(9).build();

      const dbf = new DBF.Builder().field(field).build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', err => {
          assert.equal(err, 'Error: Invalid D (date) field length: 9');
        })
        .on('header', assert.fail.bind(null, 'no header events should have been emitted'))
        .on('data', assert.fail.bind(null, 'no record events should have been emitted'))
        .on('end', done);
    });

    it('field with type L and not 1 byte in length should emit error', done => {
      const field = new Field.Builder('field1', 'L').size(2).build();

      const dbf = new DBF.Builder().field(field).build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', err => {
          assert.equal(err, 'Error: Invalid L (logical) field length: 2');
        })
        .on('header', assert.fail.bind(null, 'no header events should have been emitted'))
        .on('data', assert.fail.bind(null, 'no record events should have been emitted'))
        .on('end', done);
    });

    it('field with type M and not 10 bytes in length should emit error', done => {
      const field = new Field.Builder('field1', 'M').size(11).build();

      const dbf = new DBF.Builder().field(field).build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', err => {
          assert.equal(err, 'Error: Invalid M (memo) field length: 11');
        })
        .on('header', assert.fail.bind(null, 'no header events should have been emitted'))
        .on('data', assert.fail.bind(null, 'no record events should have been emitted'))
        .on('end', done);
    });

    it('duplicate field name should emit error', done => {
      const field1 = new Field.Builder('field1', 'C').size(3).build();
      const field2 = new Field.Builder('field2', 'C').size(2).build();
      const field3 = new Field.Builder('field1', 'L').size(1).build();

      const dbf = new DBF.Builder().field(field1).field(field2).field(field3).build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', err => {
          assert.equal(err, 'Error: Duplicate field name \'field1\'');
        })
        .on('header', assert.fail.bind(null, 'no header events should have been emitted'))
        .on('data', assert.fail.bind(null, 'no record events should have been emitted'))
        .on('end', done);
    });
  });

  describe('successful parsing', () => {
    it('all fields should be parsed', (done) => {
      const field1 = new Field.Builder('field1', 'C').size(157).precision(104).workAreaId(119).prodMDXFieldFlag(true).build();
      const field2 = new Field.Builder('field2', 'C').size(13).precision(12).workAreaId(120).prodMDXFieldFlag(false).build();

      const dbf = new DBF.Builder()
        .date(new Date(1997, 6, 25))
        .hasProductionMDXFile(0x01)
        .languageDriverId(17)
        .field(field1)
        .field(field2)
        .build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', err => {
          assert.fail(`no error should have been thrown: ${err}`);
        })
        .on('header', actualHeader => {
          assert.deepEqual(actualHeader, {
            version: 139,
            dateOfLastUpdate: new Date(1997, 5, 25),
            numberOfRecords: 0,
            numberOfHeaderBytes: 97,
            numberOfBytesInRecord: 171,
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
        .on('data', assert.fail.bind(null, 'no record events should have been emitted'))
        .on('end', done);

    });

    it('header received 1 byte at a time should successfully parse', done => {
      const field = new Field.Builder('field', 'C').size(1).precision(104).workAreaId(119).prodMDXFieldFlag(true).build();

      const dbf = new DBF.Builder()
        .date(new Date(1997, 6, 25))
        .hasProductionMDXFile(0x01)
        .languageDriverId(17)
        .field(field)
        .build();

      const entireBuffer = dbf.buffer;

      const readableStream = new Readable();
      for (let i = 0; i < entireBuffer.length; i+=1) {
        readableStream.push(entireBuffer.slice(i, i+1));
      }
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', err => {
          assert.fail(`no error should have been thrown: ${err}`);
        })
        .on('header', actualHeader => {
          assert.deepEqual(actualHeader, {
            version: 139,
            dateOfLastUpdate: new Date(1997, 5, 25),
            numberOfRecords: 0,
            numberOfHeaderBytes: 65,
            numberOfBytesInRecord: 2,
            hasProductionMDXFile: 0x01,
            langaugeDriverId: 17,
            fields: [
              {
                name: 'field',
                type: 'C',
                length: 1,
                precision: 104,
                workAreaId: 119,
                isIndexedInMDXFile: true
              }
            ]
          });
        })
        .on('data', assert.fail.bind(null, 'no record events should have been emitted'))
        .on('end', done);
    });
  });
});

describe('record parsing', () => {
  describe('anomalous file conditions', () => {
    it('no fields and no records should produce no errors', done => {
      const dbf = new DBF.Builder().build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
        .on('data', assert.fail.bind(null, 'no data events should have been emitted'))
        .on('end', () => {
          done();
        });
    });

    it('fields but no records should emit neither \'error\' nor \'data\' events', done => {
      const field1 = new Field.Builder('field1', 'C').size(25).build();
      const field2 = new Field.Builder('field2', 'C').size(30).build();

      const dbf = new DBF.Builder().field(field1).field(field2).build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
        .on('data', assert.fail.bind(null, 'no data events should have been emitted'))
        .on('end', () => {
          done();
        });
    });

    it('records but no fields should emit \'data\' events', done => {
      const record1 = new Record.Builder().build();
      const record2 = new Record.Builder().build();
      const record3 = new Record.Builder().build();

      const dbf = new DBF.Builder().record(record1).record(record2).record(record3).build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      let count = 0;

      readableStream
        .pipe(new YADBF())
        .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
        .on('data', (record) => {
          assert.deepEqual(record, {
            '@meta': {
              deleted: false
            }
          });
          count+=1;
        })
        .on('end', () => {
          assert.equal(count, 3);
          done();
        });
    });

  });

  describe('fields', () => {
    it('all non-deleted fields and records should be parsed', done => {
      const field1 = new Field.Builder('field1', 'C').size(25).build();
      const field2 = new Field.Builder('field2', 'C').size(30).build();

      const record1 = new Record.Builder()
        .field('record 1 field 1 value', field1)
        .field('record 1 field 2 value', field2)
        .build();

      const record2 = new Record.Builder()
        .field('record 2 field 1 value', field1)
        .field('record 2 field 2 value', field2)
        .deleted()
        .build();

      const record3 = new Record.Builder()
        .field('record 3 field 1 value', field1)
        .field('record 3 field 2 value', field2)
        .build();

      const dbf = new DBF.Builder()
        .field(field1)
        .field(field2)
        .record(record1)
        .record(record2)
        .record(record3)
        .build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      const records = [
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
      ];

      readableStream
        .pipe(new YADBF())
        .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
        .on('data', record => assert.deepEqual(record, records.shift()))
        .on('end', () => {
          assert.equal(records.length, 0);
          done();
        });
    });

    it('an error should be emitted when first byte of record is not a space or asterisk', done => {
      const field = new Field.Builder('field', 'C').size(25).build();

      const record = new Record.Builder()
        .deleted('#')
        .field('record 1 field 1 value', field)
        .build();

      const dbf = new DBF.Builder()
        .field(field)
        .record(record)
        .build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('header', header => assert.ok('header should have been emitted'))
        .on('error', err => {
          assert.equal(err, 'Invalid deleted record value: #');
        })
        .on('data', assert.fail.bind(null, 'no record events should have been emitted'))
        .on('end', done);
    });

    it('an error should be emitted when file does not end with 0x1A', done => {
      const field = new Field.Builder('field', 'C').size(25).build();

      const record = new Record.Builder()
        .field('record 1 field 1 value', field)
        .build();

      const dbf = new DBF.Builder()
        .field(field)
        .record(record)
        .endOfFile('Z')
        .build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('header', header => assert.ok('header was emitted'))
        .on('data', record => assert.ok('record was emitted'))
        .on('error', err => {
          assert.equal(err, 'Last byte of file is not end-of-file marker');
        })
        .on('end', done);
    });

    it('an error should be emitted when last character is not 0x1A', done => {
      const field = new Field.Builder('field', 'C').size(25).build();

      const record = new Record.Builder()
        .field('record 1 field 1 value', field)
        .build();

      const dbf = new DBF.Builder()
        .field(field)
        .record(record)
        .endOfFile(0x1b)
        .build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('header', header => assert.ok('header was emitted'))
        .on('data', record => assert.ok('record was emitted'))
        .on('error', err => {
          assert.equal(err, 'Last byte of file is not end-of-file marker');
        })
        .on('end', done);
    });

  });

  describe('C-type field parsing', () => {
    it('values should right-trimmed but not left-trimmed', done => {
      const field = new Field.Builder('field', 'C').size(11).build();

      const record = new Record.Builder()
        .field('  value   ', field)
        .build();

      const dbf = new DBF.Builder()
        .field(field)
        .record(record)
        .build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
        .on('data', record => assert.equal(record.field, '  value'))
        .on('end', done);
    });
  });

  describe('D-type field parsing', () => {
    it('D-type fields should be parsed as dates', done => {
      const field = new Field.Builder('field', 'D').build();

      const record = new Record.Builder()
        .field('19520719', field)
        .build();

      const dbf = new DBF.Builder()
        .field(field)
        .record(record)
        .build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
        .on('data', record => assert.deepEqual(record, {
          '@meta': {
            deleted: false
          },
          field: new Date(1952, 7, 19)
        }))
        .on('end', done);
    });
  });

  describe('L-type field parsing', () => {
    ['Y', 'y', 'T', 't'].forEach(truthy_value => {
      it(`L-type fields should treat '${truthy_value}' as true`, done => {
        const field = new Field.Builder('field', 'L').build();

        const record = new Record.Builder()
          .field(truthy_value, field)
          .build();

        const dbf = new DBF.Builder()
          .field(field)
          .record(record)
          .build();

        const readableStream = new Readable();
        readableStream.push(dbf.buffer);
        readableStream.push(null);

        readableStream
          .pipe(new YADBF())
          .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
          .on('data', record => assert.equal(record.field, true))
          .on('end', done);
      });
    });

    ['N', 'n', 'F', 'f'].forEach(falsey_value => {
      it(`L-type fields should treat '${falsey_value}' as false`, done => {
        const field = new Field.Builder('field', 'L').build();

        const record = new Record.Builder()
          .field(falsey_value, field)
          .build();

        const dbf = new DBF.Builder()
          .field(field)
          .record(record)
          .build();

        const readableStream = new Readable();
        readableStream.push(dbf.buffer);
        readableStream.push(null);

        readableStream
          .pipe(new YADBF())
          .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
          .on('data', record => assert.equal(record.field, false))
          .on('end', done);
      });
    });

    it('L-type fields should treat \'?\' as undefined', done => {
      const field = new Field.Builder('field', 'L').build();

      const record = new Record.Builder()
        .field('?', field)
        .build();

      const dbf = new DBF.Builder()
        .field(field)
        .record(record)
        .build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
        .on('data', record => assert.equal(record.field, undefined))
        .on('end', done);
    });

    it('L-type fields should treat \' \' as undefined', done => {
      const field = new Field.Builder('field', 'L').build();
      const record = new Record.Builder()
        .field(' ', field)
        .build();
      const dbf = new DBF.Builder()
        .field(field)
        .record(record)
        .build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
        .on('data', record => assert.equal(record.field, undefined))
        .on('end', done);
    });    

    it('L-type fields should emit error on unknown fields', done => {
      const field = new Field.Builder('field', 'L').build();

      const record = new Record.Builder()
        .field('R', field)
        .build();

      const dbf = new DBF.Builder()
        .field(field)
        .record(record)
        .build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', err => {
          assert.equal(err, 'Invalid L-type field value: R');
        })
        .on('data', assert.fail.bind(null, 'no record events should have been emitted'))
        .on('end', done);
    });
  });

  describe('F/N-type field parsing', () => {
    it('F-type field', done => {
      const field = new Field.Builder('field', 'F').size(19).build();

      const record = new Record.Builder()
        .field('123.45678', field)
        .build();

      const dbf = new DBF.Builder()
        .field(field)
        .record(record)
        .build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
        .on('data', record => {
          assert.equal(record.field, 123.45678);
        })
        .on('end', done);
    });

    it('N-type field', done => {
      const field = new Field.Builder('field', 'N').size(19).build();

      const record = new Record.Builder()
        .field('123.45678', field)
        .build();

      const dbf = new DBF.Builder()
        .field(field)
        .record(record)
        .build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
        .on('data', record => {
          assert.equal(record.field, 123.45678);
        })
        .on('end', done);
    });
  });

  describe('M-type field parsing', () => {
    it('field values consisting of all numbers should be accepted', done => {
      const field = new Field.Builder('field', 'M').build();

      const record = new Record.Builder()
        .field('1357924680', field)
        .build();

      const dbf = new DBF.Builder()
        .field(field)
        .record(record)
        .build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
        .on('data', record => {
          assert.deepEqual(record, {
            '@meta': {
              deleted: false
            },
            field: '1357924680'
          });
        })
        .on('end', done);
    });

    it('field values consisting of all spaces should be accepted', done => {
      const field = new Field.Builder('field', 'M').build();

      const record = new Record.Builder()
        .field('          ', field)
        .build();

      const dbf = new DBF.Builder()
        .field(field)
        .record(record)
        .build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
        .on('data', record => {
          assert.deepEqual(record, {
            '@meta': {
              deleted: false
            },
            field: '          '
          });
        })
        .on('end', done);

    });

    it('field values not entirely number or spaces should emit error', done => {
      const field = new Field.Builder('field', 'M').build();

      const record = new Record.Builder()
        .field('     4    ', field)
        .build();

      const dbf = new DBF.Builder()
        .field(field)
        .record(record)
        .build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      readableStream
        .pipe(new YADBF())
        .on('error', err => {
          assert.equal(err, 'Invalid M-type field value: \'     4    \'');
        })
        .on('data', assert.fail.bind(null, 'no record events should have been emitted'))
        .on('end', done);
    });
  });
});

describe('options', () => {
  describe('deleted flag', () => {
    it('non-boolean deleted should emit error', done => {
      [null, {}, [], 'this is not a boolean', 17, 17.3, NaN, Infinity].forEach(deleted => {
        assert.throws(() => {
          const yadbf = new YADBF({ deleted: deleted });
        }, /^Error: deleted must be a boolean$/);
      });
      done();
    });

    it('deleted=true should include deleted records', done => {
      const field1 = new Field.Builder('field1', 'C').size(25).build();
      const field2 = new Field.Builder('field2', 'C').size(30).build();

      const record1 = new Record.Builder()
        .field('record 1 field 1 value', field1)
        .field('record 1 field 2 value', field2)
        .build();

      const record2 = new Record.Builder()
        .deleted()
        .field('record 2 field 1 value', field1)
        .field('record 2 field 2 value', field2)
        .build();

      const record3 = new Record.Builder()
        .field('record 3 field 1 value', field1)
        .field('record 3 field 2 value', field2)
        .build();

      const dbf = new DBF.Builder()
        .field(field1)
        .field(field2)
        .record(record1)
        .record(record2)
        .record(record3)
        .build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      const records = [
        {
          '@meta': {
            deleted: false
          },
          field1: 'record 1 field 1 value',
          field2: 'record 1 field 2 value'
        },
        {
          '@meta': {
            deleted: true
          },
          field1: 'record 2 field 1 value',
          field2: 'record 2 field 2 value'
        },
        {
          '@meta': {
            deleted: false
          },
          field1: 'record 3 field 1 value',
          field2: 'record 3 field 2 value'
        }
      ];

      readableStream
        .pipe(new YADBF({ deleted: true }))
        .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
        .on('data', record => assert.deepEqual(record, records.shift()))
        .on('end', () => {
          assert.equal(records.length, 0);
          done();
        });
    });
  });

  describe('pagination', () => {
    it('negative offset should emit error', done => {
      assert.throws(() => {
        const yadbf = new YADBF({ offset: -1 });
      }, /^Error: offset must be a non-negative integer$/);
      done();
    });

    it('non-integer offset should emit error', done => {
      [null, {}, [], 'this is not an integer', 17.3, false, NaN, Infinity].forEach(offset => {
        assert.throws(() => {
          const yadbf = new YADBF({ offset: offset });
        }, /^Error: offset must be a non-negative integer$/);
      });
      done();
    });

    it('negative size should emit error', done => {
      assert.throws(() => {
        const yadbf = new YADBF({ size: -1 });
      }, /^Error: size must be a non-negative integer$/);
      done();
    });

    it('non-integer size should emit error', done => {
      [null, {}, [], 'this is not an integer', false, 17.3, NaN, Infinity].forEach(size => {
        assert.throws(() => {
          const yadbf = new YADBF({ size: size });
        }, /^Error: size must be a non-negative integer$/);
      });
      done();
    });

    it('offset not supplied should return records from the first', done => {
      const field = new Field.Builder('field', 'C').size(1).build();

      const record1 = new Record.Builder()
        .field('a', field)
        .build();

      const record2 = new Record.Builder()
        .field('b', field)
        .build();

      const record3 = new Record.Builder()
        .field('c', field)
        .build();

      const dbf = new DBF.Builder()
        .field(field)
        .record(record1)
        .record(record2)
        .record(record3)
        .build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      const records = [];

      readableStream
        .pipe(new YADBF({ size: 2 }))
        .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
        .on('data', record => records.push(record))
        .on('end', () => {
          assert.deepEqual(records.map(r => r.field), [ 'a', 'b' ]);
          done();
        });
    });

    it('size not supplied should default to all records from offset onward', done => {
      const field = new Field.Builder('field', 'C').size(1).build();

      const record1 = new Record.Builder()
        .field('a', field)
        .build();

      const record2 = new Record.Builder()
        .field('b', field)
        .build();

      const record3 = new Record.Builder()
        .field('c', field)
        .build();

      const dbf = new DBF.Builder()
        .field(field)
        .record(record1)
        .record(record2)
        .record(record3)
        .build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      const records = [];

      readableStream
        .pipe(new YADBF({ offset: 1 }))
        .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
        .on('data', record => records.push(record))
        .on('end', () => {
          assert.deepEqual(records.map(r => r.field), [ 'b', 'c' ]);
          done();
        });
    });

    it('size 0 should return 0 documents despite 3 records in stream', done => {
      const field = new Field.Builder('field', 'C').size(1).build();

      const record1 = new Record.Builder()
        .field('a', field)
        .build();

      const record2 = new Record.Builder()
        .field('b', field)
        .build();

      const record3 = new Record.Builder()
        .field('c', field)
        .build();

      const dbf = new DBF.Builder()
        .field(field)
        .record(record1)
        .record(record2)
        .record(record3)
        .build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      const records = [];

      readableStream
        .pipe(new YADBF({ size: 0 }))
        .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
        .on('data', assert.fail.bind(null, 'no data events should have been emitted'))
        .on('end', () => {
          done();
        });
    });

    it('offset and size both supplied should be honored but not include deleted records', done => {
      const field = new Field.Builder('field', 'C').size(1).build();

      const record1 = new Record.Builder().field('a', field).deleted().build();
      const record2 = new Record.Builder().field('b', field).build();
      const record3 = new Record.Builder().field('c', field).build();
      const record4 = new Record.Builder().field('d', field).deleted().build();
      const record5 = new Record.Builder().field('e', field).build();
      const record6 = new Record.Builder().field('f', field).build();

      const dbf = new DBF.Builder()
        .field(field)
        .record(record1)
        .record(record2)
        .record(record3)
        .record(record4)
        .record(record5)
        .record(record6)
        .build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      const records = [];

      readableStream
        .pipe(new YADBF({ offset: 1, size: 2 }))
        .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
        .on('data', record => records.push(record) )
        .on('end', () => {
          assert.deepEqual(records.map(r => r.field), [ 'c', 'e' ]);
          done();
        });
    });

    it('specified size greater than number of records available should everything available', done => {
      const field = new Field.Builder('field', 'C').size(1).build();

      const record1 = new Record.Builder().field('a', field).build();
      const record2 = new Record.Builder().field('b', field).build();
      const record3 = new Record.Builder().field('c', field).build();

      const dbf = new DBF.Builder()
        .field(field)
        .record(record1)
        .record(record2)
        .record(record3)
        .build();

      const readableStream = new Readable();
      readableStream.push(dbf.buffer);
      readableStream.push(null);

      const records = [];

      readableStream
        .pipe(new YADBF({ size: 17 }))
        .on('error', assert.fail.bind(null, 'no error events should have been emitted'))
        .on('data', record => records.push(record) )
        .on('end', () => {
          assert.deepEqual(records.map(r => r.field), [ 'a', 'b', 'c' ]);
          done();
        });
    });
  });
});
