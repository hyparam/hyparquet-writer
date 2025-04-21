
/**
 * @import {DecodedArray, ParquetType} from 'hyparquet/src/types.js'
 * @import {Writer} from '../src/types.js'
 * @param {Writer} writer
 * @param {DecodedArray} values
 * @param {ParquetType} type
 */
export function writePlain(writer, values, type) {
  if (type === 'BOOLEAN') {
    writePlainBoolean(writer, values)
  } else if (type === 'INT32') {
    writePlainInt32(writer, values)
  } else if (type === 'INT64') {
    writePlainInt64(writer, values)
  } else if (type === 'FLOAT') {
    writePlainFloat(writer, values)
  } else if (type === 'DOUBLE') {
    writePlainDouble(writer, values)
  } else if (type === 'BYTE_ARRAY') {
    writePlainByteArray(writer, values)
  } else if (type === 'FIXED_LEN_BYTE_ARRAY') {
    writePlainByteArrayFixed(writer, values)
  } else {
    throw new Error(`parquet unsupported type: ${type}`)
  }
}

/**
 * @param {Writer} writer
 * @param {DecodedArray} values
 */
function writePlainBoolean(writer, values) {
  let currentByte = 0

  for (let i = 0; i < values.length; i++) {
    if (typeof values[i] !== 'boolean') throw new Error('parquet expected boolean value')
    const bitOffset = i % 8

    if (values[i]) {
      currentByte |= 1 << bitOffset
    }

    // once we've packed 8 bits or are at a multiple of 8, we write out the byte
    if (bitOffset === 7) {
      writer.appendUint8(currentByte)
      currentByte = 0
    }
  }

  // if the array length is not a multiple of 8, write the leftover bits
  if (values.length % 8 !== 0) {
    writer.appendUint8(currentByte)
  }
}

/**
 * @param {Writer} writer
 * @param {DecodedArray} values
 */
function writePlainInt32(writer, values) {
  for (const value of values) {
    if (!Number.isSafeInteger(value)) throw new Error('parquet expected integer value')
    writer.appendInt32(value)
  }
}

/**
 * @param {Writer} writer
 * @param {DecodedArray} values
 */
function writePlainInt64(writer, values) {
  for (const value of values) {
    if (typeof value !== 'bigint') throw new Error('parquet expected bigint value')
    writer.appendInt64(value)
  }
}

/**
 * @param {Writer} writer
 * @param {DecodedArray} values
 */
function writePlainFloat(writer, values) {
  for (const value of values) {
    if (typeof value !== 'number') throw new Error('parquet expected number value')
    writer.appendFloat32(value)
  }
}

/**
 * @param {Writer} writer
 * @param {DecodedArray} values
 */
function writePlainDouble(writer, values) {
  for (const value of values) {
    if (typeof value !== 'number') throw new Error('parquet expected number value')
    writer.appendFloat64(value)
  }
}

/**
 * @param {Writer} writer
 * @param {DecodedArray} values
 */
function writePlainByteArray(writer, values) {
  for (const value of values) {
    let bytes = value
    if (typeof bytes === 'string') {
      // convert string to Uint8Array
      bytes = new TextEncoder().encode(value)
    }
    if (!(bytes instanceof Uint8Array)) {
      throw new Error('parquet expected Uint8Array value')
    }
    writer.appendUint32(bytes.length)
    writer.appendBytes(bytes)
  }
}

/**
 * @param {Writer} writer
 * @param {DecodedArray} values
 */
function writePlainByteArrayFixed(writer, values) {
  for (const value of values) {
    if (!(value instanceof Uint8Array)) throw new Error('parquet expected Uint8Array value')
    writer.appendBytes(value)
  }
}
