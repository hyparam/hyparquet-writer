
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
  } else if (type === 'DOUBLE') {
    writePlainDouble(writer, values)
  } else if (type === 'BYTE_ARRAY') {
    writePlainByteArray(writer, values)
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
    const bitOffset = i % 8

    if (values[i]) {
      currentByte |= 1 << bitOffset
    }

    // Once we've packed 8 bits or are at a multiple of 8, we write out the byte
    if (bitOffset === 7) {
      writer.appendUint8(currentByte)
      currentByte = 0
    }
  }

  // If the array length is not a multiple of 8, write the leftover bits
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
    writer.appendInt32(value)
  }
}

/**
 * @param {Writer} writer
 * @param {DecodedArray} values
 */
function writePlainInt64(writer, values) {
  for (const value of values) {
    writer.appendInt64(value)
  }
}

/**
 * @param {Writer} writer
 * @param {DecodedArray} values
 */
function writePlainDouble(writer, values) {
  for (const value of values) {
    writer.appendFloat64(value)
  }
}

/**
 * @param {Writer} writer
 * @param {DecodedArray} values
 */
function writePlainByteArray(writer, values) {
  for (const value of values) {
    if (!(value instanceof Uint8Array)) throw new Error('BYTE_ARRAY must be Uint8Array')
    writer.appendUint32(value.length)
    writer.appendBytes(value)
  }
}
