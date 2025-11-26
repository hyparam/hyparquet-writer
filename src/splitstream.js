/**
 * Write values using BYTE_STREAM_SPLIT encoding.
 * This encoding writes all first bytes of values, then all second bytes, etc.
 * Can improve compression for floating-point and fixed-width numeric data.
 *
 * @import {DecodedArray, ParquetType} from 'hyparquet'
 * @import {Writer} from '../src/types.js'
 * @param {Writer} writer
 * @param {DecodedArray} values
 * @param {ParquetType} type
 * @param {number | undefined} typeLength
 */
export function writeByteStreamSplit(writer, values, type, typeLength) {
  const count = values.length

  // Get bytes from values based on type
  /** @type {Uint8Array} */
  let bytes
  /** @type {number} */
  let width
  if (type === 'FLOAT') {
    const typed = values instanceof Float32Array ? values : new Float32Array(numberArray(values))
    bytes = new Uint8Array(typed.buffer, typed.byteOffset, typed.byteLength)
    width = 4
  } else if (type === 'DOUBLE') {
    const typed = values instanceof Float64Array ? values : new Float64Array(numberArray(values))
    bytes = new Uint8Array(typed.buffer, typed.byteOffset, typed.byteLength)
    width = 8
  } else if (type === 'INT32') {
    const typed = values instanceof Int32Array ? values : new Int32Array(numberArray(values))
    bytes = new Uint8Array(typed.buffer, typed.byteOffset, typed.byteLength)
    width = 4
  } else if (type === 'INT64') {
    const typed = bigIntArray(values)
    bytes = new Uint8Array(typed.buffer, typed.byteOffset, typed.byteLength)
    width = 8
  } else if (type === 'FIXED_LEN_BYTE_ARRAY') {
    if (!typeLength) throw new Error('parquet byte_stream_split missing type_length')
    width = typeLength
    bytes = new Uint8Array(count * width)
    for (let i = 0; i < count; i++) {
      bytes.set(values[i], i * width)
    }
  } else {
    throw new Error(`parquet byte_stream_split unsupported type: ${type}`)
  }

  // Write bytes in column format (all byte 0 from all values, then byte 1, etc.)
  for (let b = 0; b < width; b++) {
    for (let i = 0; i < count; i++) {
      writer.appendUint8(bytes[i * width + b])
    }
  }
}

/**
 * @param {DecodedArray} values
 * @returns {number[]}
 */
function numberArray(values) {
  if (Array.isArray(values) && values.every(v => typeof v === 'number')) {
    return values
  }
  throw new Error('Expected number array for BYTE_STREAM_SPLIT encoding')
}

/**
 * @param {DecodedArray} values
 * @returns {BigInt64Array}
 */
function bigIntArray(values) {
  if (values instanceof BigInt64Array) return values
  if (Array.isArray(values) && values.every(v => typeof v === 'bigint')) {
    return new BigInt64Array(values)
  }
  throw new Error('Expected bigint array for BYTE_STREAM_SPLIT encoding')
}

