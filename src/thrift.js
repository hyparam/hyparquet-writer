import { CompactType } from 'hyparquet/src/thrift.js'

/**
 * Serialize a JS object in TCompactProtocol format.
 *
 * Expects keys named like "field_1", "field_2", etc. in ascending order.
 *
 * @import {ThriftType} from 'hyparquet/src/types.js'
 * @import {Writer} from '../src/types.js'
 * @param {Writer} writer
 * @param {Record<string, any>} data
 */
export function serializeTCompactProtocol(writer, data) {
  let lastFid = 0
  // write each field
  for (const [key, value] of Object.entries(data)) {
    if (value === undefined) continue

    // we expect key = "field_N" so we can extract N as the field ID
    const fid = parseInt(key.replace(/^field_/, ''), 10)
    if (Number.isNaN(fid)) {
      throw new Error(`thrift invalid field name: ${key}. Expected "field_###".`)
    }

    // write the field-begin header
    const type = getCompactTypeForValue(value)
    const delta = fid - lastFid
    if (delta <= 0) {
      throw new Error(`thrift non-monotonic field ID: fid=${fid}, lastFid=${lastFid}`)
    }
    // High nibble = delta, low nibble = type
    writer.appendUint8(delta << 4 | type)

    // Write the field content itself
    writeElement(writer, type, value)

    lastFid = fid
  }

  // Finally write STOP
  writer.appendUint8(CompactType.STOP)
}

/**
 * Deduce a TCompactProtocol type from the JS value
 *
 * @param {any} value
 * @returns {number} CompactType
 */
function getCompactTypeForValue(value) {
  if (value === true) return CompactType.TRUE
  if (value === false) return CompactType.FALSE
  if (Number.isInteger(value)) return CompactType.I32
  if (typeof value === 'number') return CompactType.DOUBLE
  if (typeof value === 'bigint') return CompactType.I64
  if (typeof value === 'string') return CompactType.BINARY
  if (value instanceof Uint8Array) return CompactType.BINARY
  if (Array.isArray(value)) return CompactType.LIST
  if (value && typeof value === 'object') return CompactType.STRUCT
  throw new Error(`Cannot determine thrift compact type for: ${value}`)
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
  if (type === CompactType.TRUE) return
  if (type === CompactType.FALSE) return
  if (type === CompactType.BYTE && typeof value === 'number') {
    writer.appendUint8(value)
  } else if (type === CompactType.I32 && typeof value === 'number') {
    const zigzag = value << 1 ^ value >> 31
    writer.appendVarInt(zigzag)
  } else if (type === CompactType.I64 && typeof value === 'bigint') {
    // For 64-bit (bigint) we do (value << 1n) ^ (value >> 63n) in zigzag
    const zigzag = value << 1n ^ value >> 63n
    writer.appendVarBigInt(zigzag)
  } else if (type === CompactType.DOUBLE && typeof value === 'number') {
    writer.appendFloat64(value)
  } else if (type === CompactType.BINARY && typeof value === 'string') {
    // store length as a varint, then raw bytes
    const bytes = new TextEncoder().encode(value)
    writer.appendVarInt(bytes.length)
    writer.appendBytes(bytes)
  } else if (type === CompactType.BINARY && value instanceof Uint8Array) {
    // store length as a varint, then raw bytes
    writer.appendVarInt(value.byteLength)
    writer.appendBytes(value)
  } else if (type === CompactType.LIST && Array.isArray(value)) {
    // Must store (size << 4) | elementType
    // We'll guess the element type from the first element
    const size = value.length
    if (size === 0) {
      // (0 << 4) | type for an empty list – pick BYTE arbitrarily
      writer.appendUint8(0 << 4 | CompactType.BYTE)
      return
    }

    // TODO: Check for heterogeneous lists?
    const elemType = getCompactTypeForValue(value[0])

    const sizeNibble = size > 14 ? 15 : size
    writer.appendUint8(sizeNibble << 4 | elemType)
    if (size > 14) {
      writer.appendVarInt(size)
    }

    // Special trick for booleans in a list
    if (elemType === CompactType.TRUE || elemType === CompactType.FALSE) {
      // Write each boolean as a single 0 or 1 byte
      for (const v of value) {
        writer.appendUint8(v ? 1 : 0)
      }
    } else {
      // Otherwise write them out normally
      for (const v of value) {
        writeElement(writer, elemType, v)
      }
    }
  } else if (type === CompactType.STRUCT && typeof value === 'object') {
    // Recursively write sub-fields as "field_N: val", end with STOP
    let lastFid = 0
    for (const [k, v] of Object.entries(value)) {
      if (v === undefined) continue

      const fid = parseInt(k.replace(/^field_/, ''), 10)
      if (Number.isNaN(fid)) {
        throw new Error(`Invalid sub-field name: ${k}. Expected "field_###"`)
      }
      const t = getCompactTypeForValue(v)
      const delta = fid - lastFid
      if (delta <= 0) {
        throw new Error(`Non-monotonic fid in struct: fid=${fid}, lastFid=${lastFid}`)
      }
      writer.appendUint8(delta << 4 | t & 0x0f)
      writeElement(writer, t, v)
      lastFid = fid
    }
    // Write STOP
    writer.appendUint8(CompactType.STOP)
  } else {
    throw new Error(`unhandled type in writeElement: ${type} for value ${value}`)
  }
}
