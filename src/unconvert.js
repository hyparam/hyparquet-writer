
/**
 * Convert from rich to primitive types.
 *
 * @import {DecodedArray, SchemaElement} from 'hyparquet'
 * @param {SchemaElement} schemaElement
 * @param {DecodedArray} values
 * @returns {DecodedArray}
 */
export function unconvert(schemaElement, values) {
  const ctype = schemaElement.converted_type
  if (ctype === 'DATE') {
    return values.map(v => v.getTime())
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
 * @param {SchemaElement} schema
 * @returns {Uint8Array | undefined}
 */
export function unconvertMetadata(value, schema) {
  if (value === undefined || value === null) return undefined
  const { type, converted_type } = schema
  if (type === 'BOOLEAN') return new Uint8Array([value ? 1 : 0])
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
  if (type === 'INT64' && converted_type === 'TIMESTAMP_MILLIS' && value instanceof Date) {
    const buffer = new ArrayBuffer(8)
    new DataView(buffer).setBigInt64(0, BigInt(value.getTime()), true)
    return new Uint8Array(buffer)
  }
  throw new Error(`unsupported type for statistics: ${type} with value ${value}`)
}
