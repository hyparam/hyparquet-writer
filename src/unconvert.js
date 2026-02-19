import { toJson } from 'hyparquet'
import { geojsonToWkb } from './wkb.js'

const dayMillis = 86400000 // 1 day in milliseconds
/**
 * @import {DecodedArray, SchemaElement, Statistics} from 'hyparquet'
 * @import {MinMaxType} from 'hyparquet/src/types.js'
 * @import {ThriftObject} from '../src/types.js'
 */

/**
 * Convert from rich to primitive types.
 *
 * @param {SchemaElement} element
 * @param {DecodedArray} values
 * @returns {DecodedArray}
 */
export function unconvert(element, values) {
  const { type, converted_type: ctype, logical_type: ltype } = element
  if (ctype === 'DECIMAL') {
    const factor = 10 ** (element.scale || 0)
    return values.map(v => {
      if (v === null || v === undefined) return v
      if (typeof v !== 'number') throw new Error('DECIMAL must be a number')
      return unconvertDecimal(element, BigInt(Math.round(v * factor)))
    })
  }
  if (ctype === 'DATE') {
    return Array.from(values).map(v => {
      if (v === null || v === undefined) return v
      if (v instanceof Date) return v.getTime() / dayMillis
      return v
    })
  }
  if (ctype === 'TIMESTAMP_MILLIS') {
    return Array.from(values).map(v => {
      if (v === null || v === undefined) return v
      if (v instanceof Date) return BigInt(v.getTime())
      return BigInt(v)
    })
  }
  if (ctype === 'TIMESTAMP_MICROS') {
    return Array.from(values).map(v => {
      if (v === null || v === undefined) return v
      if (v instanceof Date) return BigInt(v.getTime() * 1000)
      return BigInt(v)
    })
  }
  if (ctype === 'JSON') {
    if (!Array.isArray(values)) throw new Error('JSON must be an array')
    const encoder = new TextEncoder()
    return values.map(v => v === undefined ? undefined : encoder.encode(JSON.stringify(toJson(v))))
  }
  if (ctype === 'UTF8') {
    if (!Array.isArray(values)) throw new Error('strings must be an array')
    const encoder = new TextEncoder()
    return values.map(v => typeof v === 'string' ? encoder.encode(v) : v)
  }
  if (ltype?.type === 'FLOAT16') {
    if (type !== 'FIXED_LEN_BYTE_ARRAY') throw new Error('FLOAT16 must be FIXED_LEN_BYTE_ARRAY type')
    if (element.type_length !== 2) throw new Error('FLOAT16 expected type_length to be 2 bytes')
    return Array.from(values).map(unconvertFloat16)
  }
  if (ltype?.type === 'UUID') {
    if (!Array.isArray(values)) throw new Error('UUID must be an array')
    if (type !== 'FIXED_LEN_BYTE_ARRAY') throw new Error('UUID must be FIXED_LEN_BYTE_ARRAY type')
    if (element.type_length !== 16) throw new Error('UUID expected type_length to be 16 bytes')
    return values.map(unconvertUuid)
  }
  if (ltype?.type === 'TIMESTAMP') {
    return Array.from(values).map(v => {
      if (v === null || v === undefined) return v
      if (v instanceof Date) {
        const millis = BigInt(v.getTime())
        if (ltype.unit === 'NANOS') return millis * 1_000_000n
        if (ltype.unit === 'MICROS') return millis * 1_000n
        return millis // MILLIS (default)
      }
      return BigInt(v)
    })
  }
  if (ltype?.type === 'GEOMETRY' || ltype?.type === 'GEOGRAPHY') {
    if (!Array.isArray(values)) throw new Error('geometry must be an array')
    return values.map(v => v && geojsonToWkb(v))
  }
  return values
}

/**
 * @param {Uint8Array | string | undefined} value
 * @returns {Uint8Array | undefined}
 */
function unconvertUuid(value) {
  if (value === undefined || value === null) return
  if (value instanceof Uint8Array) return value
  if (typeof value === 'string') {
    const uuidRegex = /^[0-9a-f]{8}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{12}$/i
    if (!uuidRegex.test(value)) {
      throw new Error('UUID must be a valid UUID string')
    }
    value = value.replace(/-/g, '').toLowerCase()
    const bytes = new Uint8Array(16)
    for (let i = 0; i < 16; i++) {
      bytes[i] = parseInt(value.slice(i * 2, i * 2 + 2), 16)
    }
    return bytes
  }
  throw new Error('UUID must be a string or Uint8Array')
}

/**
 * Uncovert from rich type to byte array for metadata statistics.
 *
 * @param {MinMaxType | undefined} value
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
  if (type === 'INT64' && converted_type === 'TIMESTAMP_MICROS' && value instanceof Date) {
    const buffer = new ArrayBuffer(8)
    new DataView(buffer).setBigInt64(0, BigInt(value.getTime() * 1000), true)
    return new Uint8Array(buffer)
  }
  if (type === 'INT64' && element.logical_type?.type === 'TIMESTAMP' && value instanceof Date) {
    const millis = BigInt(value.getTime())
    const { unit } = element.logical_type
    let bigintValue = millis
    if (unit === 'NANOS') bigintValue = millis * 1_000_000n
    else if (unit === 'MICROS') bigintValue = millis * 1_000n
    const buffer = new ArrayBuffer(8)
    new DataView(buffer).setBigInt64(0, bigintValue, true)
    return new Uint8Array(buffer)
  }
  throw new Error(`unsupported type for statistics: ${type} with value ${value}`)
}

/**
 * @param {Statistics} stats
 * @param {SchemaElement} element
 * @returns {ThriftObject}
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
      const sign = byte & 0x80
      if (!sign && value === 0n || sign && value === -1n) {
        break
      }
    }
  }

  return new Uint8Array(bytes)
}

/**
 * @param {number | undefined} value
 * @returns {Uint8Array | undefined}
 */
export function unconvertFloat16(value) {
  if (value === undefined || value === null) return
  if (typeof value !== 'number') throw new Error('parquet float16 expected number value')
  if (Number.isNaN(value)) return new Uint8Array([0x00, 0x7e])

  const sign = value < 0 || Object.is(value, -0) ? 1 : 0
  const abs = Math.abs(value)

  // infinities
  if (!isFinite(abs)) return new Uint8Array([0x00, sign << 7 | 0x7c])

  // ±0
  if (abs === 0) return new Uint8Array([0x00, sign << 7])

  // write as f32 to get raw bits
  const buf = new ArrayBuffer(4)
  new Float32Array(buf)[0] = abs
  const bits32 = new Uint32Array(buf)[0]

  let exp32 = bits32 >>> 23 & 0xff
  let mant32 = bits32 & 0x7fffff

  // convert 32‑bit exponent to unbiased, then to 16‑bit
  exp32 -= 127

  // handle numbers too small for a normal 16‑bit exponent
  if (exp32 < -14) {
    // sub‑normal: shift mantissa so that result = mant * 2^-14
    const shift = -14 - exp32
    mant32 = (mant32 | 0x800000) >> shift + 13

    // round‑to‑nearest‑even
    if (mant32 & 1) mant32 += 1

    const bits16 = sign << 15 | mant32
    return new Uint8Array([bits16 & 0xff, bits16 >> 8])
  }

  // overflow
  if (exp32 > 15) return new Uint8Array([0x00, sign << 7 | 0x7c])

  // normal number
  let exp16 = exp32 + 15
  mant32 = mant32 + 0x1000 // add rounding bit

  // handle mantissa overflow after rounding
  if (mant32 & 0x800000) {
    mant32 = 0
    if (++exp16 === 31) // became infinity
      return new Uint8Array([0x00, sign << 7 | 0x7c])
  }

  const bits16 = sign << 15 | exp16 << 10 | mant32 >> 13
  return new Uint8Array([bits16 & 0xff, bits16 >> 8])
}
