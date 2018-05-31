const { Transform } = require('stream');

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

// valid M-type value regex
const validMTypeValueRegex = /^(\d{10}| {10})$/;

function parseHeaderField(fieldBytes, val, i) {
  const field = fieldBytes.slice(i*32, i*32+32);

  const length = field.readUInt8(16);
  if (length === 255) {
    throw new Error('Field length must be less than 255');
  }

  const type = field.toString('utf-8', 11, 12);
  if (!supportedFieldTypes.has(type)) {
    throw new Error(`Field type must be one of: ${Array.from(supportedFieldTypes).join(', ')}`);
  }

  // check types & lengths
  if (type === 'D' && length !== 8) {
    throw new Error(`Invalid D (date) field length: ${length}`);
  }
  if (type === 'L' && length !== 1) {
    throw new Error(`Invalid L (logical) field length: ${length}`);
  }
  if (type === 'M' && length !== 10) {
    throw new Error(`Invalid M (memo) field length: ${length}`);
  }

  const isIndexedInMDXFile = field.readUInt8(31);
  if (isIndexedInMDXFile > 1) {
    throw new Error(`Invalid indexed in production MDX file value: ${isIndexedInMDXFile}`);
  }

  return {
    name: field.toString('utf-8', 0, 10).replace(/\0/g, ''),
    type: type,
    length: length,
    precision: field.readUInt8(17),
    workAreaId: field.readUInt16LE(18),
    isIndexedInMDXFile: isIndexedInMDXFile === 1
  };

}

function parseHeader(chunk) {
  const versionByte = chunk.readUInt8(0);
  if (!supportedVersions.has(versionByte)) {
    throw new Error(`Unsupported version: ${versionByte}`);
  }

  const numberOfHeaderBytes = chunk.readUInt16LE(8);
  // the number of header bytes should be 1 when modded with 32
  if (numberOfHeaderBytes % 32 !== 1) {
    throw new Error(`Invalid number of header bytes: ${numberOfHeaderBytes}`);
  }

  // there are 32 bytes per header field + 1 byte for terminator + 32 bytes for the initial header
  const numberOfFields = (numberOfHeaderBytes-32-1)/32;

  const fieldBytes = chunk.slice(32, numberOfHeaderBytes);
  // emit an error if the header bytes does not end with 0x0D (per spec)
  if (fieldBytes.readUInt8(numberOfHeaderBytes-32-1) !== 0x0D) {
    throw new Error(`Invalid field descriptor array terminator at byte ${numberOfHeaderBytes}`);
  }

  const encryptionByte = chunk.readUInt8(15);
  // if the source is encrypted, then emit an error
  if (encryptionByte === 1) {
    throw new Error('Encryption flag is set, cannot process');
  }
  // valid values for the encryption byte are 0x00 and 0x01, emit an error otherwise
  if (encryptionByte > 1) {
    throw new Error(`Invalid encryption flag value: ${encryptionByte}`);
  }

  const hasProductionMDXFile = chunk.readUInt8(28);
  // production MDX file existence value must be 0x01 or 0x02 (per spec)
  if (hasProductionMDXFile > 1) {
    throw new Error(`Invalid production MDX file existence value: ${hasProductionMDXFile}`);
  }

  // construct and return the header
  return {
    version: versionByte,
    dateOfLastUpdate: new Date(
      1900 + chunk.readUInt8(1),
      chunk.readUInt8(2) - 1,
      chunk.readUInt8(3)
    ),
    numberOfRecords: chunk.readInt32LE(4),
    numberOfHeaderBytes: numberOfHeaderBytes,
    numberOfBytesInRecord: chunk.readInt16LE(10),
    hasProductionMDXFile: hasProductionMDXFile,
    langaugeDriverId: chunk.readUInt8(29),
    fields: Array.from( {length: numberOfFields }, parseHeaderField.bind(null, fieldBytes))
  };

}

function convertToObject(header, chunk) {
  const record = {
    '@meta': {
      deleted: isDeleted(chunk)
    }
  };

  // keep track of how far we're into the record byte-wise
  let offset = 1;

  return header.fields.reduce((record, field) => {
    const value = chunk.toString('utf-8', offset, offset+field.length);

    if (field.type === 'D') {
      // read D-type fields as dates
      record[field.name] = new Date(
        value.substr(0, 4),
        value.substr(4, 2),
        value.substr(6, 2)
      );

    }
    else if (field.type === 'L') {
      if (truthyValues.has(value)) {
        record[field.name] = true;
      } else if (falseyValues.has(value)) {
        record[field.name] = false;
      } else if (value !== '?') {
        throw `Invalid L-type field value: ${value}`;
      }

    }
    else if (field.type === 'F' || field.type === 'N') {
      record[field.name] = parseFloat(value);
    }
    else if (field.type === 'C') {
      record[field.name] = value.replace(/[\u0000 ]+$/, '');
    }
    else if (field.type === 'M') {
      if (!validMTypeValueRegex.test(value)) {
        throw `Invalid M-type field value: '${value}'`;
      } else {
        record[field.name] = value;
      }

    }

    offset += field.length;

    return record;

  }, record);

}

function isDeleted(chunk) {
  const firstByte = chunk.readUInt8(0, 1);

  if (firstByte === 0x20) { // ' '
    return false;
  }
  if (firstByte === 0x2A) { // '*'
    return true;
  }

  throw `Invalid deleted record value: ${String.fromCharCode(firstByte)}`;

}

function hasEnoughBytesForHeader(chunk) {
  return chunk.length < 32 || chunk.length < chunk.readUInt16LE(8);
}

module.exports = (options) => new Transform({
  readableObjectMode: true,
  final(callback) {
    if (!this.header) {
      const numberOfBytes = this.unconsumedBytes ? this.unconsumedBytes.length : 0;

      this.destroy(`Unable to parse first 32 bytes from header, found ${numberOfBytes} byte(s)`);
    }

    return callback();
  },
  transform(chunk, encoding, callback) {
    // if the header hasn't been parsed yet, do so now and emit it
    if (!this.header) {
      if (this.unconsumedBytes) {
        chunk = Buffer.concat([this.unconsumedBytes, chunk]);
        delete this.unconsumedBytes;
      }

      // if there aren't enough bytes to read the header, save off the accumulated
      //  bytes for later use
      if (hasEnoughBytesForHeader(chunk)) {
        this.unconsumedBytes = chunk;
        return callback();
      }

      try {
        this.header = parseHeader(chunk);
 
        // emit the header for outside consumption
        this.emit('header', this.header);

        // remove the header bytes from the beginning of the chunk (for easier bookkeeping)
        chunk = chunk.slice(this.header.numberOfHeaderBytes);

        // keep track of how many records have been made readable
        this.numberOfRecordsPushed = 0;

      } catch (err) {
        this.destroy(err);
        return callback();
      }
    }

    // if there were leftover bytes from the previous chunk, prepend them to the current chunk
    if (this.unconsumedBytes) {
      chunk = Buffer.concat( [this.unconsumedBytes, chunk], this.unconsumedBytes.length + chunk.length );
      delete this.unconsumedBytes;
    }

    // calculate the number of records available in this chunk
    // there will most likely be a fragment of the next record at the end, so floor it
    const numberOfRecordsInThisChunk = Math.floor(chunk.length / this.header.numberOfBytesInRecord);

    // slice up the chunk into record-size bites, then iterate and push
    Array.from({length: numberOfRecordsInThisChunk}, (val, i) => chunk.slice(
        i * this.header.numberOfBytesInRecord, 
        i * this.header.numberOfBytesInRecord + this.header.numberOfBytesInRecord)
    ).forEach(chunk => {
      try {
        const record = convertToObject(this.header, chunk);

        // don't push deleted records (yet)
        if (!record['@meta'].deleted) {
          this.push(record);
        }

      } catch (err) {
        this.destroy(err);
      }
    });

    // increment total number of records pushed by the number pushed in this chunk
    this.numberOfRecordsPushed += numberOfRecordsInThisChunk;

    // anything leftover after all the records in this chunk should be saved off til the next iteration
    this.unconsumedBytes = chunk.slice(this.header.numberOfBytesInRecord * numberOfRecordsInThisChunk);

    // if all the records have been emitted, proceed with shutdown
    if (this.numberOfRecordsPushed === this.header.numberOfRecords && 
        this.unconsumedBytes.length === 1) {
      // throw an error if the last byte isn't the expected EOF marker
      if (this.unconsumedBytes.readUInt8(0) !== 0x1A) {
        this.destroy('Last byte of file is not end-of-file marker');
      }

      // otherwise clear up unconsumedBytes and signal end-of-stream
      delete this.unconsumedBytes;
      this.push(null);
    }

    callback();
  }
});
