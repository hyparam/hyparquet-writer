// TCompactProtocol types
const CompactType = {
  STOP: 0,
  TRUE: 1,
  FALSE: 2,
  BYTE: 3,
  I16: 4,
  I32: 5,
  I64: 6,
  DOUBLE: 7,
  BINARY: 8,
  LIST: 9,
  SET: 10,
  MAP: 11,
  STRUCT: 12,
  UUID: 13,
}

/**
 * Serialize a JS object in TCompactProtocol format.
 *
 * Expects keys named like "field_1", "field_2", etc. in ascending order.
 *
 * @import {Writer} from '../src/types.js'
 * @param {Writer} writer
 * @param {Record<string, any>} data
 */
export function serializeTCompactProtocol(writer, data) {
  let lastFid = 0
  // Write each field
  for (const [key, value] of Object.entries(data)) {
    if (value === undefined) continue

    // We expect key = "field_N" so we can extract N as the field ID
    const fid = parseInt(key.replace(/^field_/, ''), 10)
    if (Number.isNaN(fid)) {
      throw new Error(`Invalid field name: ${key}. Expected "field_###" format.`)
    }

    // Figure out which compact type to use
    const type = getCompactTypeForValue(value)

    // Write the field-begin header: (delta << 4) | type
    const delta = fid - lastFid
    if (delta <= 0) {
      throw new Error(`Non-monotonic field ID. fid=${fid}, lastFid=${lastFid}`)
    }
    // High nibble = delta, low nibble = type
    writer.appendUint8(delta << 4 | type & 0x0f)

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
  if (value === true) {
    return CompactType.TRUE
  }
  if (value === false) {
    return CompactType.FALSE
  }
  if (typeof value === 'number') {
    // We'll store integer as I32, otherwise DOUBLE
    return Number.isInteger(value) ? CompactType.I32 : CompactType.DOUBLE
  }
  if (typeof value === 'bigint') {
    return CompactType.I64
  }
  if (typeof value === 'string') {
    // Possibly treat 32-hex as a 16-byte UUID
    if (/^[0-9a-fA-F]{32}$/.test(value)) {
      return CompactType.UUID
    }
    return CompactType.BINARY
  }
  if (value instanceof Uint8Array) {
    return CompactType.BINARY
  }
  if (Array.isArray(value)) {
    return CompactType.LIST
  }
  if (value && typeof value === 'object') {
    return CompactType.STRUCT
  }
  throw new Error(`Cannot determine thrift compact type for: ${value}`)
}

/**
 * Write a single value of a given compact type.
 *
 * @param {Writer} writer
 * @param {number} type
 * @param {any} value
 */
function writeElement(writer, type, value) {
  switch (type) {
  case CompactType.TRUE:
  case CompactType.FALSE:
    return // true/false is stored in the type
  case CompactType.BYTE:
    writer.appendUint8(value)
    return
  case CompactType.I16:
  case CompactType.I32: {
    // ZigZag -> varint
    // For 32-bit int: zigzag = (n << 1) ^ (n >> 31)
    const zigzag = value << 1 ^ value >> 31
    writer.appendVarInt(zigzag)
    return
  }
  case CompactType.I64: {
    // For 64-bit (bigint) we do (value << 1n) ^ (value >> 63n) in zigzag
    const n = BigInt(value)
    const zigzag = n << 1n ^ n >> 63n
    writer.appendVarBigInt(zigzag)
    return
  }
  case CompactType.DOUBLE:
    writer.appendFloat64(value)
    return
  case CompactType.BINARY: {
    // store length as a varint, then raw bytes
    let bytes
    if (typeof value === 'string') {
      bytes = new TextEncoder().encode(value)
    } else {
      // e.g. Uint8Array
      bytes = value
    }
    writer.appendVarInt(bytes.length)
    writer.appendBuffer(bytes)
    return
  }
  case CompactType.LIST: {
    // Must store (size << 4) | elementType
    // We'll guess the element type from the first element
    const arr = value
    const size = arr.length
    if (size === 0) {
      // (0 << 4) | type for an empty list – pick BYTE arbitrarily
      writer.appendUint8(0 << 4 | CompactType.BYTE)
      return
    }

    // TODO: Check for heterogeneous lists?
    const elemType = getCompactTypeForValue(arr[0])

    const sizeNibble = size > 14 ? 15 : size
    writer.appendUint8(sizeNibble << 4 | elemType)
    if (size > 14) {
      writer.appendVarInt(size)
    }

    // Special trick for booleans in a list
    if (elemType === CompactType.TRUE || elemType === CompactType.FALSE) {
      // Write each boolean as a single 0 or 1 byte
      for (const v of arr) {
        writer.appendUint8(v ? 1 : 0)
      }
    } else {
      // Otherwise write them out normally
      for (const v of arr) {
        writeElement(writer, elemType, v)
      }
    }
    return
  }
  case CompactType.STRUCT: {
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
    return
  }
  case CompactType.UUID: {
    // Expect a 32-hex string. Write 16 bytes
    if (typeof value !== 'string' || value.length !== 32) {
      throw new Error(`Expected 32-hex string for UUID, got ${value}`)
    }
    for (let i = 0; i < 16; i++) {
      const byte = parseInt(value.slice(i * 2, i * 2 + 2), 16)
      writer.appendUint8(byte)
    }
    return
  }

  default:
    throw new Error(`Unhandled type in writeElement: ${type}`)
  }
}
