const { Transform } = require('stream');
const Iconv = require('iconv-lite');

class YADBF extends Transform {

  /**
   * 
   * @param {options} [options]
   * @param {number} [options.offset=0]
   * @param {number} [options.size=Infinity]
   * @param {boolean} [options.deleted=false] whether deleted records should be included on the output
   * @param {string} [options.encoding="utf-8"] 
   * @param {object} [options.customFieldParsers] an object whose keys are field names and their values are functions to be called for custom-parsing them. The parameters passed are the Buffer value of the field and and object describing the field
   * @param {object} [options.quirks] 
   * @param {booelan} [options.quirks.typeL_allowUnknownValues] treat any unknown value as unknown instead of throwing
   * @param {booelan} [options.quirks.typeM_allowLeftSpacePadding] allow M-type fields to be left-padded with spaces
   * @param {booelan} [options.quirks.ignoreUnknownEncryptionByte] ignores if the encryption flag is different then 0 or 1
   * @param {booelan} [options.quirks.allowFieldLength255] allow field lengths to be equal to 255
   */
  constructor(options = {}) {
    super({ readableObjectMode: true });

    // create an empty buffer to simplify logic later
    this.unconsumedBytes = Buffer.alloc(0);

    this.offset = validateOffset(options.offset);
    this.size = validateSize(options.size);
    this.includeDeletedRecords = validateDeleted(options.deleted);
    this.encoding = validateEncoding(options.encoding);
    this.customFieldParsers = options.customFieldParsers || {};
    this.quirks = options.quirks || {};

    // keep track of how many records have been made readable (used for end-of-stream detection)
    this.totalRecordCount = 0;

    // keep track of how many records *could* have been pushed (used for pagination)
    this.eligibleRecordCount = 0;
  }

  _final(callback) {
    if (!this.header) {
      const numberOfBytes = this.unconsumedBytes ? this.unconsumedBytes.length : 0;

      this.destroy(`Unable to parse first 32 bytes from header, found ${numberOfBytes} byte(s)`);
    }

    return callback();
  }

  _transform(chunk, encoding, callback) {
    // append the chunk to unconsumed bytes for easier bookkeeping
    this.unconsumedBytes = Buffer.concat([this.unconsumedBytes, chunk]);

    // if the header hasn't been parsed yet, do so now and emit it
    if (!this.header) {
      // if there aren't enough bytes to read the header, save off the accumulated
      //  bytes for later use and return
      if (!hasEnoughBytesForHeader(this.unconsumedBytes)) {
        return callback();
      }

      // otherwise, attempt to parse the header
      try {
        this.header = parseHeader.call(this, this.unconsumedBytes);

        // emit the header for outside consumption
        this.emit('header', this.header);

        // remove the header bytes from the beginning of the chunk (for easier bookkeeping)
        this.unconsumedBytes = this.unconsumedBytes.slice(this.header.numberOfHeaderBytes);

      } catch (err) {
        this.destroy(err);
        return callback();
      }
    }

    // process records from the unconsumed bytes
    while (hasEnoughBytesForRecord(this.unconsumedBytes, this.header) && moreRecordsAreExpected.bind(this)()) {
      // get enough bytes for the record
      const recordSizedChunk = this.unconsumedBytes.slice(0, this.header.numberOfBytesInRecord);

      try {
        const record = convertToRecord.call(this, recordSizedChunk, this.header, this.encoding);

        // only push if it's eligble for output and within the pagination params
        if (isEligibleForOutput(record, this.includeDeletedRecords)) {
          if (isWithinPage(this.eligibleRecordCount, this.offset, this.size)) {
            this.push(record);
          }

          // increment total # of records pushed for pagination check
          this.eligibleRecordCount += 1;
        }

        // increment total # of records consumed for end-of-stream check
        this.totalRecordCount += 1;

        // remove the slice from the unconsumed bytes
        this.unconsumedBytes = this.unconsumedBytes.slice(recordSizedChunk.length);

      } catch (err) {
        this.destroy(err);
        return callback();
      }

    }

    // if all the records have been emitted, proceed with shutdown
    if (allRecordsHaveBeenProcessed(this.header.numberOfRecords, this.totalRecordCount) &&
      aSingleByteRemains(this.unconsumedBytes)) {
      // throw an error if the last byte isn't the expected EOF marker
      if (!firstByteIsEOFMarker(this.unconsumedBytes)) {
        this.destroy('Last byte of file is not end-of-file marker');
      }

      // otherwise clear up unconsumedBytes and signal end-of-stream
      delete this.unconsumedBytes;
      this.push(null);
    }

    callback();
  }

}

// see: https://github.com/infused/dbf/blob/master/lib/dbf/table.rb
const supportedVersions = new Set([
  0x03, // FoxBASE+/Dbase III plus, no memo
  0x83, // FoxBASE+/dBASE III PLUS, with memo
  0xF5, // FoxPro 2.x (or earlier) with memo
  0x8B, // dBASE IV with memo
  0x8E  // ?
]);
const supportedFieldTypes = new Set(['C', 'D', 'F', 'L', 'M', 'N']);
const truthyValues = new Set(['Y', 'y', 'T', 't']);
const falseyValues = new Set(['N', 'n', 'F', 'f']);

// valid M-type value regex (10 digits or 10 spaces)
const validMTypeValueRegex = /^(\d{10}| {10})$/;
const validMTypeValuePaddedRegex = /^ {0,10}\d{0,10}$/;

// type handlers keyed by the single character type designator
const typeHandlers = {
  D(value) {
    return new Date(
      value.substr(0, 4)
      + '-'
      + value.substr(4, 2)
      + '-'
      + value.substr(6, 2)
    );
  },
  L(value) {
    if (truthyValues.has(value)) {
      return true;
    } else if (falseyValues.has(value)) {
      return false;
    } else if (value !== '?' && value !== ' ' && !this.quirks.typeL_allowUnknownValues) { // '?' or ' ' means undefined
      throw new Error(`Invalid L-type field value: ${value}`);
    }
  },
  F(value) {
    return parseFloat(value);
  },
  N(value) {
    return parseFloat(value);
  },
  C(value) {
    return value.replace(/[\u0000 ]+$/, '');
  },
  M(value) {
    let regex = this.quirks.typeM_allowLeftSpacePadding ? validMTypeValuePaddedRegex : validMTypeValueRegex;
    if (!regex.test(value)) {
      throw new Error(`Invalid M-type field value: '${value}'`);
    } else {
      return value;
    }
  }
};

// returns true if enough bytes have been read to parse the entire header
function hasEnoughBytesForHeader(chunk) {
  return chunk.length >= 32 && chunk.length >= chunk.readUInt16LE(8);
}

// returns true if enough bytes have been read to parse a record
function hasEnoughBytesForRecord(chunk, header) {
  return chunk.length >= header.numberOfBytesInRecord;
}

// returns true if the number of processed records is less than the number of declared records
function moreRecordsAreExpected() {
  return this.totalRecordCount < this.header.numberOfRecords;
}

// returns true if record is not deleted or deleted records should be included
function isEligibleForOutput(record, includeDeletedRecords) {
  return !record['@meta'].deleted || !!includeDeletedRecords;
}

// returns true if count is within the page size/offset combination
function isWithinPage(count, offset, size) {
  return count >= offset && count < offset + size;
}

// returns true if the actual number of records processed matches the number of expected records
function allRecordsHaveBeenProcessed(expectedNumberOfRecords, numberOfRecordsProcessed) {
  return expectedNumberOfRecords === numberOfRecordsProcessed;
}

// returns true if there's exactly 1 byte in a buffer
function aSingleByteRemains(unconsumedBytes) {
  return unconsumedBytes.length === 1;
}

// returns true if the first byte of a buffer is the EOF marker
function firstByteIsEOFMarker(unconsumedBytes) {
  return unconsumedBytes.readUInt8(0) === 0x1A;
}

// convert the buffer to a header object
function parseHeader(buffer) {
  const versionByte = buffer.readUInt8(0);
  if (!supportedVersions.has(versionByte)) {
    throw new Error(`Unsupported version: ${versionByte}`);
  }

  const numberOfHeaderBytes = buffer.readUInt16LE(8);
  // the number of header bytes should be 1 when modded with 32
  if (numberOfHeaderBytes % 32 !== 1) {
    throw new Error(`Invalid number of header bytes: ${numberOfHeaderBytes}`);
  }

  // there are 32 bytes per header field + 1 byte for terminator + 32 bytes for the initial header
  const numberOfFields = (numberOfHeaderBytes - 32 - 1) / 32;

  const fieldBytes = buffer.slice(32, numberOfHeaderBytes);
  // emit an error if the header bytes does not end with 0x0D (per spec)
  if (fieldBytes.readUInt8(numberOfHeaderBytes - 32 - 1) !== 0x0D) {
    throw new Error(`Invalid field descriptor array terminator at byte ${numberOfHeaderBytes}`);
  }

  const encryptionByte = buffer.readUInt8(15);
  // if the source is encrypted, then emit an error
  if (encryptionByte === 1) {
    throw new Error('Encryption flag is set, cannot process');
  }
  // valid values for the encryption byte are 0x00 and 0x01, emit an error otherwise
  if (encryptionByte > 1 && !this.quirks.ignoreUnknownEncryptionByte) {
    throw new Error(`Invalid encryption flag value: ${encryptionByte}`);
  }

  const hasProductionMDXFile = buffer.readUInt8(28);
  // production MDX file existence value must be 0x01 or 0x02 (per spec)
  if (hasProductionMDXFile > 1) {
    throw new Error(`Invalid production MDX file existence value: ${hasProductionMDXFile}`);
  }

  // construct and return the header
  const header = {
    version: versionByte,
    dateOfLastUpdate: new Date(
      1900 + buffer.readUInt8(1),
      buffer.readUInt8(2) - 1,
      buffer.readUInt8(3)
    ),
    numberOfRecords: buffer.readInt32LE(4),
    numberOfHeaderBytes: numberOfHeaderBytes,
    numberOfBytesInRecord: buffer.readInt16LE(10),
    hasProductionMDXFile: hasProductionMDXFile,
    langaugeDriverId: buffer.readUInt8(29),
    fields: Array.from({ length: numberOfFields }, parseHeaderField.bind(this, fieldBytes))
  };

  // if there are any duplicate field names, throw an error
  header.fields.reduce((allFieldNames, field) => {
    if (allFieldNames.has(field.name)) {
      throw new Error(`Duplicate field name '${field.name}'`);
    }
    return allFieldNames.add(field.name);
  }, new Set());

  return header;
}

// parses up 32 bytes from `fieldBytes` into a valid field definition
function parseHeaderField(fieldBytes, val, i) {
  const field = fieldBytes.slice(i * 32, i * 32 + 32);

  // extract the field length from the 16th byte
  const length = field.readUInt8(16);
  if (length === 255 && !this.quirks.allowFieldLength255) {
    throw new Error('Field length must be less than 255');
  }

  // extract the field type from the 11th byte
  const type = field.toString('utf-8', 11, 12);
  if (!supportedFieldTypes.has(type)) {
    throw new Error(`Field type must be one of: ${Array.from(supportedFieldTypes).join(', ')}`);
  }

  // validate that certain types have expected lengths
  if (type === 'D' && length !== 8) {
    throw new Error(`Invalid D (date) field length: ${length}`);
  }
  if (type === 'L' && length !== 1) {
    throw new Error(`Invalid L (logical) field length: ${length}`);
  }
  if (type === 'M' && length !== 10) {
    throw new Error(`Invalid M (memo) field length: ${length}`);
  }

  // i have no idea what this is, but read it anyway since it might be of use
  const isIndexedInMDXFile = field.readUInt8(31);
  if (isIndexedInMDXFile > 1) {
    throw new Error(`Invalid indexed in production MDX file value: ${isIndexedInMDXFile}`);
  }

  // return an object representing the field definition
  return {
    name: field.toString('utf-8', 0, 10).replace(/\0/g, ''),
    type: type,
    length: length,
    precision: field.readUInt8(17),
    workAreaId: field.readUInt16LE(18),
    isIndexedInMDXFile: isIndexedInMDXFile === 1
  };
}

// converts a record-sized chunk into an object based on the metadata available in `header`
function convertToRecord(chunk, header, encoding) {
  const record = {
    '@meta': {
      deleted: isDeleted(chunk)
    }
  };

  // keep track of how far we're into the record byte-wise
  // start at 1 since the 0th byte is the deleted flag
  let byteOffset = 1;

  header.fields.forEach(field => {
    // read the value out with given encoding
    const bValue = chunk.slice(byteOffset, byteOffset + field.length);

    // assign the field into the record
    if (this.customFieldParsers[field.name]) {
      record[field.name] = this.customFieldParsers[field.name](bValue, field);
    } else {
      const value = Iconv.decode(bValue, encoding);
      record[field.name] = typeHandlers[field.type].call(this, value);
    }


    // update where the next field starts
    byteOffset += field.length;

  });

  return record;
}

// determines if the first byte of a chunk is a valid deleted flag, or throws an error otherwise
function isDeleted(chunk) {
  const firstByte = chunk.readUInt8(0, 1);

  if (firstByte === 0x20) { // ' '
    return false;
  }
  if (firstByte === 0x2A) { // '*'
    return true;
  }

  throw new Error(`Invalid deleted record value: ${String.fromCharCode(firstByte)}`);
}

// validates that `offset` is a non-negative integer, defaulting to `Infinity` if not supplied
function validateOffset(offset) {
  if (offset === undefined) {
    return 0;
  }

  if (offset < 0 || !Number.isInteger(offset)) {
    throw new Error('offset must be a non-negative integer');
  }

  return offset;
}

// validates that `size` is a non-negative integer, defaulting to `Infinity` if not supplied
function validateSize(size) {
  if (size === undefined) {
    return Infinity;
  }

  if (size < 0 || !Number.isInteger(size)) {
    throw new Error('size must be a non-negative integer');
  }

  return size;
}

// validates that `deleted` is a boolean, defaulting to `false` if not supplied
function validateDeleted(deleted) {
  if (deleted === undefined) {
    return false;
  }

  if (deleted !== true && deleted !== false) {
    throw new Error('deleted must be a boolean');
  }

  return deleted;
}

// validates that `encoding` exists
function validateEncoding(encoding) {
  if (encoding === undefined) {
    return 'utf-8';
  }

  if (!Iconv.encodingExists(encoding)) {
    throw new Error(`encoding not recognized: '${encoding}'`);
  }

  return encoding;
}

module.exports = YADBF;
