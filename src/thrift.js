/**
 * @import {ThriftType} from 'hyparquet/src/types.js'
 * @import {Writer} from '../src/types.js'
 */

// TCompactProtocol types
const STOP = 0
const TRUE = 1
const FALSE = 2
const BYTE = 3
const I32 = 5
const I64 = 6
const DOUBLE = 7
const BINARY = 8
const LIST = 9
const STRUCT = 12

/**
 * Serialize a JS object in TCompactProtocol format.
 *
 * Expects keys named like "field_1", "field_2", etc. in ascending order.
 *
 * @param {Writer} writer
 * @param {{ [key: `field_${number}`]: any }} data
 */
export function serializeTCompactProtocol(writer, data) {
  writeElement(writer, STRUCT, data)
}

/**
 * Write a single value of a given compact type.
 *
 * @param {Writer} writer
 * @param {number} type
 * @param {ThriftType} value
 */
function writeElement(writer, type, value) {
  // true/false is stored in the type
  if (type === TRUE) return
  if (type === FALSE) return
  if (type === BYTE && typeof value === 'number') {
    writer.appendUint8(value)
  } else if (type === I32 && typeof value === 'number') {
    writer.appendZigZag(value)
  } else if (type === I64 && typeof value === 'bigint') {
    writer.appendZigZag(value)
  } else if (type === DOUBLE && typeof value === 'number') {
    writer.appendFloat64(value)
  } else if (type === BINARY && typeof value === 'string') {
    // store length as a varint, then raw bytes
    const bytes = new TextEncoder().encode(value)
    writer.appendVarInt(bytes.length)
    writer.appendBytes(bytes)
  } else if (type === BINARY && value instanceof Uint8Array) {
    // store length as a varint, then raw bytes
    writer.appendVarInt(value.byteLength)
    writer.appendBytes(value)
  } else if (type === LIST && Array.isArray(value)) {
    // Guess the element type from the first element
    const elemType = getCompactTypeForList(value)

    // Header: size << 4 | elementType
    if (value.length > 14) {
      writer.appendUint8(15 << 4 | elemType)
      writer.appendVarInt(value.length)
    } else {
      writer.appendUint8(value.length << 4 | elemType)
    }

    if (elemType === FALSE) {
      // Special case for boolean list
      for (const v of value) {
        writer.appendUint8(v ? 1 : 0)
      }
    } else {
      for (const v of value) {
        writeElement(writer, elemType, v)
      }
    }
  } else if (type === STRUCT && typeof value === 'object') {
    // write struct fields
    let lastFid = 0
    for (const [k, v] of Object.entries(value)) {
      if (v === undefined) continue

      const fid = parseInt(k.replace(/^field_/, ''), 10)
      if (Number.isNaN(fid)) {
        throw new Error(`thrift invalid field name: ${k}. Expected "field_###"`)
      }
      const t = getCompactTypeForValue(v)
      const delta = fid - lastFid
      if (delta <= 0) {
        throw new Error(`thrift non-monotonic field id: fid=${fid}, lastFid=${lastFid}`)
      }
      if (delta > 15) {
        writer.appendUint8(t)
        writer.appendZigZag(fid)
      } else {
        writer.appendUint8(delta << 4 | t)
      }
      writeElement(writer, t, v)
      lastFid = fid
    }
    // end struct
    writer.appendUint8(STOP)
  } else {
    throw new Error(`thrift invalid type ${type} for value ${value}`)
  }
}

/**
 * Infer type from JS value
 *
 * @param {any} value
 * @returns {number} CompactType
 */
function getCompactTypeForValue(value) {
  if (value === true) return TRUE
  if (value === false) return FALSE
  if (Number.isInteger(value)) return I32
  if (typeof value === 'number') return DOUBLE
  if (typeof value === 'bigint') return I64
  if (typeof value === 'string') return BINARY
  if (value instanceof Uint8Array) return BINARY
  if (Array.isArray(value)) return LIST
  if (value && typeof value === 'object') return STRUCT
  throw new Error(`Cannot determine thrift compact type for: ${value}`)
}

/**
 * Infer type for list elements, expand types as needed
 *
 * @param {any[]} value
 * @returns {number} CompactType
 */
function getCompactTypeForList(value) {
  let elemType = 0
  for (const v of value) {
    let t = getCompactTypeForValue(v)
    if (t === TRUE) t = FALSE // booleans map to FALSE
    if (!elemType) elemType = t // first element
    if (elemType === DOUBLE && t === I32) t = DOUBLE // expand int to float
    if (elemType === I32 && t === DOUBLE) elemType = DOUBLE // expand int to float
    if (t !== elemType) {
      throw new Error(`thrift invalid type for list element: ${v} (expected type ${elemType})`)
    }
  }
  return elemType ?? BYTE // BYTE for empty list
}
