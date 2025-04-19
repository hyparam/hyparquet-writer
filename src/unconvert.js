const dayMillis = 86400000 // 1 day in milliseconds

/**
 * Convert from rich to primitive types.
 *
 * @import {DecodedArray, SchemaElement, Statistics} from 'hyparquet'
 * @param {SchemaElement} element
 * @param {DecodedArray} values
 * @returns {DecodedArray}
 */
export function unconvert(element, values) {
  const ctype = element.converted_type
  if (ctype === 'DECIMAL') {
    const factor = 10 ** (element.scale || 0)
    return values.map(v => {
      if (v === null || v === undefined) return v
      if (typeof v !== 'number') throw new Error('DECIMAL must be a number')
      return unconvertDecimal(element, BigInt(Math.round(v * factor)))
    })
  }
  if (ctype === 'DATE') {
    return Array.from(values).map(v => v && v.getTime() / dayMillis)
  }
  if (ctype === 'TIMESTAMP_MILLIS') {
    return Array.from(values).map(v => v && BigInt(v.getTime()))
  }
  if (ctype === 'TIMESTAMP_MICROS') {
    return Array.from(values).map(v => v && BigInt(v.getTime() * 1000))
  }
  if (ctype === 'JSON') {
    if (!Array.isArray(values)) throw new Error('JSON must be an array')
    const encoder = new TextEncoder()
    return values.map(v => encoder.encode(JSON.stringify(v)))
  }
  if (ctype === 'UTF8') {
    if (!Array.isArray(values)) throw new Error('strings must be an array')
    const encoder = new TextEncoder()
    return values.map(v => encoder.encode(v))
  }
  return values
}

/**
 * Uncovert from rich type to byte array for metadata statistics.
 *
 * @param {import('hyparquet/src/types.js').MinMaxType | undefined} value
 * @param {SchemaElement} element
 * @returns {Uint8Array | undefined}
 */
export function unconvertMinMax(value, element) {
  if (value === undefined || value === null) return undefined
  const { type, converted_type } = element
  if (type === 'BOOLEAN') return new Uint8Array([value ? 1 : 0])
  if (converted_type === 'DECIMAL') {
    if (typeof value !== 'number') throw new Error('DECIMAL must be a number')
    const factor = 10 ** (element.scale || 0)
    const out = unconvertDecimal(element, BigInt(Math.round(value * factor)))
    if (out instanceof Uint8Array) return out
    if (typeof out === 'number') {
      const buffer = new ArrayBuffer(4)
      new DataView(buffer).setFloat32(0, out, true)
      return new Uint8Array(buffer)
    }
    if (typeof out === 'bigint') {
      const buffer = new ArrayBuffer(8)
      new DataView(buffer).setBigInt64(0, out, true)
      return new Uint8Array(buffer)
    }
  }
  if (type === 'BYTE_ARRAY' || type === 'FIXED_LEN_BYTE_ARRAY') {
    // truncate byte arrays to 16 bytes for statistics
    if (value instanceof Uint8Array) return value.slice(0, 16)
    return new TextEncoder().encode(value.toString().slice(0, 16))
  }
  if (type === 'FLOAT' && typeof value === 'number') {
    const buffer = new ArrayBuffer(4)
    new DataView(buffer).setFloat32(0, value, true)
    return new Uint8Array(buffer)
  }
  if (type === 'DOUBLE' && typeof value === 'number') {
    const buffer = new ArrayBuffer(8)
    new DataView(buffer).setFloat64(0, value, true)
    return new Uint8Array(buffer)
  }
  if (type === 'INT32' && typeof value === 'number') {
    const buffer = new ArrayBuffer(4)
    new DataView(buffer).setInt32(0, value, true)
    return new Uint8Array(buffer)
  }
  if (type === 'INT64' && typeof value === 'bigint') {
    const buffer = new ArrayBuffer(8)
    new DataView(buffer).setBigInt64(0, value, true)
    return new Uint8Array(buffer)
  }
  if (type === 'INT32' && converted_type === 'DATE' && value instanceof Date) {
    const buffer = new ArrayBuffer(4)
    new DataView(buffer).setInt32(0, Math.floor(value.getTime() / dayMillis), true)
    return new Uint8Array(buffer)
  }
  if (type === 'INT64' && converted_type === 'TIMESTAMP_MILLIS' && value instanceof Date) {
    const buffer = new ArrayBuffer(8)
    new DataView(buffer).setBigInt64(0, BigInt(value.getTime()), true)
    return new Uint8Array(buffer)
  }
  throw new Error(`unsupported type for statistics: ${type} with value ${value}`)
}

/**
 * @param {Statistics} stats
 * @param {SchemaElement} element
 * @returns {import('../src/types.js').ThriftObject}
 */
export function unconvertStatistics(stats, element) {
  return {
    field_1: unconvertMinMax(stats.max, element),
    field_2: unconvertMinMax(stats.min, element),
    field_3: stats.null_count,
    field_4: stats.distinct_count,
    field_5: unconvertMinMax(stats.max_value, element),
    field_6: unconvertMinMax(stats.min_value, element),
    field_7: stats.is_max_value_exact,
    field_8: stats.is_min_value_exact,
  }
}

/**
 * @param {SchemaElement} element
 * @param {bigint} value
 * @returns {number | bigint | Uint8Array}
 */
export function unconvertDecimal({ type, type_length }, value) {
  if (type === 'INT32') return Number(value)
  if (type === 'INT64') return value
  if (type === 'FIXED_LEN_BYTE_ARRAY' && !type_length) {
    throw new Error('fixed length byte array type_length is required')
  }
  if (!type_length && !value) return new Uint8Array()

  const bytes = []
  while (true) {
    // extract the lowest 8 bits
    const byte = Number(value & 0xffn)
    bytes.unshift(byte)
    value >>= 8n

    if (type_length) {
      if (bytes.length >= type_length) break // fixed length
    } else {
      // for nonnegative: stop when top byte has signBit = 0 AND shifted value == 0n
      // for negative: stop when top byte has signBit = 1 AND shifted value == -1n
      const signBit = byte & 0x80
      if (!signBit && value === 0n || signBit && value === -1n) {
        break
      }
    }
  }

  return new Uint8Array(bytes)
}
