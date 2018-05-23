// http://www.dbase.com/Knowledgebase/INT/db7_file_fmt.htm
var through2 = require('through2');

// see: https://github.com/infused/dbf/blob/master/lib/dbf/table.rb
const supportedVersions = [
  0x03, // FoxBASE+/Dbase III plus, no memo
  0x83, // FoxBASE+/dBASE III PLUS, with memo
  0xF5, // FoxPro 2.x (or earlier) with memo
  0x8B, // dBASE IV with memo
  0x8E  // ?
];
const supportedFieldTypes = ['C', 'D', 'F', 'L', 'M', 'N'];
const truthyValues = ['Y', 'y', 'T', 't'];
const falseyValues = ['N', 'n', 'F', 'f'];

// valid M-type value regex
const validMTypeValueRegex = /^(\d{10}| {10})$/;

function readHeader(source) {
  const fileHeader = source.read(32);

  // bail if no header bytes could be read
  if (!fileHeader) {
    source.emit('error', 'Unable to parse first 32 bytes from header, found 0 byte(s)');
    return;
  }

  // bail if the database header could not be read
  if (fileHeader.length !== 32) {
    source.emit('error', `Unable to parse first 32 bytes from header, found ${fileHeader.length} byte(s)`);
    return;
  }

  // if the version is not supported, emit an error
  const versionByte = fileHeader.readUInt8(0);
  if (supportedVersions.indexOf(versionByte) === -1) {
    source.emit('error', `Unsupported version: ${versionByte}`);
    return;
  }

  const numberOfHeaderBytes = fileHeader.readUInt16LE(8);
  // the number of header bytes should be 1 when modded with 32
  if (numberOfHeaderBytes % 32 !== 1) {
    source.emit('error', `Invalid number of header bytes: ${numberOfHeaderBytes}`);
    return;
  }

  const fieldBytes = source.read(numberOfHeaderBytes-32);
  // emit an error if the header bytes does not end with 0x0D (per spec)
  if (fieldBytes.readUInt8(numberOfHeaderBytes-32-1) !== 0x0D) {
    source.emit('error', `Invalid field descriptor array terminator at byte ${numberOfHeaderBytes}`);
    return;
  }

  const encryptionByte = fileHeader.readUInt8(15);
  // if the source is encrypted, then emit an error
  if (encryptionByte === 1) {
    source.emit('error', 'Encryption flag is set, cannot process');
    return;
  }
  // valid values for the encryption byte are 0x00 and 0x01, emit an error otherwise
  if (encryptionByte > 1) {
    source.emit('error', `Invalid encryption flag value: ${encryptionByte}`);
    return;
  }

  const hasProductionMDXFile = fileHeader.readUInt8(28);
  // production MDX file existence value must be 0x01 or 0x02 (per spec)
  if (hasProductionMDXFile > 1) {
    source.emit('error', `Invalid production MDX file existence value: ${hasProductionMDXFile}`);
    return;
  }

  const header = {
    version: versionByte,
    dateOfLastUpdate: new Date(
      1900 + fileHeader.readUInt8(1),
      fileHeader.readUInt8(2) - 1,
      fileHeader.readUInt8(3)
    ),
    numberOfRecords: fileHeader.readInt32LE(4),
    numberOfHeaderBytes: numberOfHeaderBytes,
    numberOfBytesInRecord: fileHeader.readInt16LE(10),
    hasProductionMDXFile: hasProductionMDXFile,
    langaugeDriverId: fileHeader.readUInt8(29),
    fields: []
  };

  // there are m bytes for field definitions, where
  // m = number of header bytes - 32 (base header) - 1 (field descriptor array terminator)
  // m should be divisible by 32 since each field is described in exactly 32 bytes
  const numberOfFields = (fieldBytes.length-1)/32;

  for (let i = 0; i < numberOfFields; i+=1) {
    const field = fieldBytes.slice(i*32, i*32+32);

    const length = field.readUInt8(16);
    if (length === 255) {
      source.emit('error', 'Field length must be less than 255');
      return;
    }

    const type = field.toString('utf-8', 11, 12);
    if (supportedFieldTypes.indexOf(type) === -1) {
      source.emit('error', `Field type must be one of: ${supportedFieldTypes.join(', ')}`);
      return;
    }

    // check types & lengths
    if (type === 'D' && length !== 8) {
      source.emit('error', `Invalid D (date) field length: ${length}`);
      return;
    }
    if (type === 'L' && length !== 1) {
      source.emit('error', `Invalid L (logical) field length: ${length}`);
      return;
    }
    if (type === 'M' && length !== 10) {
      source.emit('error', `Invalid M (memo) field length: ${length}`);
      return;
    }

    const isIndexedInMDXFile = field.readUInt8(31);
    if (isIndexedInMDXFile > 1) {
      source.emit('error', `Invalid indexed in production MDX file value: ${isIndexedInMDXFile}`);
      return;
    }

    header.fields.push({
      name: field.toString('utf-8', 0, 10).replace(/\0/g, ''),
      type: type,
      length: length,
      precision: field.readUInt8(17),
      workAreaId: field.readUInt16LE(18),
      isIndexedInMDXFile: isIndexedInMDXFile === 1
    });

  }

  return header;

};

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
      if (truthyValues.indexOf(value) >= 0) {
        record[field.name] = true;
      } else if (falseyValues.indexOf(value) >= 0) {
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

module.exports = (source, options) => {
  let header;

  // read the header first, only emitting if successfully parsed
  source.once('readable', () => {
    header = readHeader(source);
    if (header) {
      source.emit('header', header);
    }
  });

  // register a handler for all subsequent events
  source.on('readable', () => {
    let chunk;

    while (null !== (chunk = source.read(header.numberOfBytesInRecord))) {
      // console.error(`read ${chunk.length} bytes`);
      // console.error(`chunk: ${chunk}`);
      try {
        if (chunk.length === header.numberOfBytesInRecord) {
          if (!isDeleted(chunk)) {
            source.emit('record', convertToObject(header, chunk));
          }
        } else if ( chunk.length === 1 && chunk.readUInt8(0) === 0x1A) {
          // check for 0x1A (end-of-file marker) when there's a single byte read
          source.emit('close');
        } else {
          source.emit('error', 'Last byte of file is not end-of-file marker');
        }

      }
      catch (err) {
        source.emit('error', err.toString());
      }

    }

  });

  return source;

};


module.exports.stream = (options) => {

  var proxy = through2({
    objectMode: false,
    highWaterMark: 16384 // 16kb
  });

  var stream = through2({
    writableObjectMode: false,
    writableHighWaterMark: 16384, // 16kb
    readableObjectMode: true,
    readableHighWaterMark: 512
  },
    proxy.write.bind(proxy),
    proxy.end.bind(proxy)
  );

  proxy.on('error', stream.emit.bind(stream, 'error'));
  proxy.on('header', stream.emit.bind(stream, 'header'));
  proxy.on('record', stream.emit.bind(stream, 'record'));
  proxy.on('record', stream.push.bind(stream));
  proxy.once('end', stream.emit.bind(stream, 'end'));

  proxy.once('readable', function(){
    process.nextTick(function(){
      process.nextTick(function(){
        process.nextTick(function(){
          process.nextTick(function(){
            var yadbf = module.exports(proxy, options);
            proxy.emit('readable');
          });
        });
      });
    });
  });

  return stream;
};
