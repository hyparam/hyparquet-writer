import { describe, expect, it } from 'vitest'
import { unconvert, unconvertMetadata } from '../src/unconvert.js'
import { convertMetadata } from 'hyparquet/src/metadata.js'

/**
 * @import {SchemaElement} from 'hyparquet'
 */
describe('unconvert', () => {
  it('should return Date objects when converted_type = DATE', () => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', converted_type: 'DATE' }
    const input = [new Date('2020-01-01T00:00:00Z'), new Date('2021-01-01T00:00:00Z')]
    const result = unconvert(schema, input)

    expect(result).toEqual([
      new Date('2020-01-01T00:00:00Z').getTime(),
      new Date('2021-01-01T00:00:00Z').getTime(),
    ])
  })

  it('should convert JSON objects to strings when converted_type = JSON', () => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', converted_type: 'JSON' }
    const input = [{ foo: 'bar' }, { hello: 'world' }]
    const result = unconvert(schema, input)

    // We check that result is an array of Uint8Arrays containing the JSON-encoded bytes
    expect(result).toHaveLength(2)
    expect(result[0]).toBeInstanceOf(Uint8Array)
    expect(new TextDecoder().decode(result[0])).toEqual(JSON.stringify({ foo: 'bar' }))
    expect(new TextDecoder().decode(result[1])).toEqual(JSON.stringify({ hello: 'world' }))
  })

  it('should convert string array to Uint8Array when converted_type = UTF8', () => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', converted_type: 'UTF8' }
    const input = ['hello', 'world']
    const result = unconvert(schema, input)

    expect(result).toHaveLength(2)
    expect(result[0]).toBeInstanceOf(Uint8Array)
    expect(new TextDecoder().decode(result[0])).toBe('hello')
    expect(new TextDecoder().decode(result[1])).toBe('world')
  })

  it('should throw an error when converted_type = UTF8 and values is not an array', () => {
    expect(() => unconvert(
      { name: 'test', converted_type: 'UTF8' },
      new Uint8Array([1, 2, 3]))
    ).toThrow('strings must be an array')
  })

  it('should throw an error when converted_type = JSON and values is not an array', () => {
    expect(() => unconvert(
      { name: 'test', converted_type: 'JSON' },
      new Uint8Array([1, 2, 3]))
    ).toThrow('JSON must be an array')
  })

  it('should return original values if there is no recognized converted_type', () => {
    const input = [1, 2, 3]
    const result = unconvert({ name: 'test' }, input)
    expect(result).toEqual(input)
  })
})

describe('unconvertMetadata', () => {
  it('should return undefined if value is undefined or null', () => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', type: 'INT32' }
    expect(unconvertMetadata(undefined, schema)).toBeUndefined()
  })

  it('should handle BOOLEAN type', () => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', type: 'BOOLEAN' }
    expect(unconvertMetadata(true, schema)).toEqual(new Uint8Array([1]))
    expect(unconvertMetadata(false, schema)).toEqual(new Uint8Array([0]))
  })

  it('should truncate BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY to 16 bytes', () => {
    // longer string to test truncation
    const longStr = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    const longStrUint8 = new TextEncoder().encode(longStr)

    // value is a Uint8Array
    const result1 = unconvertMetadata(longStrUint8, { name: 'test', type: 'BYTE_ARRAY' })
    expect(result1).toBeInstanceOf(Uint8Array)
    expect(result1?.length).toBe(16) // truncated

    // value is a string
    const result2 = unconvertMetadata(longStr, { name: 'test', type: 'FIXED_LEN_BYTE_ARRAY' })
    expect(result2).toBeInstanceOf(Uint8Array)
    expect(result2?.length).toBe(16) // truncated
  })

  it('should correctly encode FLOAT values in little-endian', () => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', type: 'FLOAT' }
    const value = 1.5
    const result = unconvertMetadata(value, schema)
    expect(result).toBeInstanceOf(Uint8Array)
    const roundtrip = convertMetadata(result, schema)
    expect(roundtrip).toEqual(1.5)
  })

  it('should correctly encode DOUBLE values in little-endian', () => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', type: 'DOUBLE' }
    const value = 1.123456789
    const result = unconvertMetadata(value, schema)
    expect(result).toBeInstanceOf(Uint8Array)
    const roundtrip = convertMetadata(result, schema)
    expect(roundtrip).toEqual(1.123456789)
  })

  it('should correctly encode INT32 values in little-endian', () => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', type: 'INT32' }
    const value = 123456
    const result = unconvertMetadata(value, schema)
    const roundtrip = convertMetadata(result, schema)
    expect(roundtrip).toEqual(123456)
  })

  it('should correctly encode INT64 values when given a bigint', () => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', type: 'INT64' }
    const value = 1234567890123456789n
    const result = unconvertMetadata(value, schema)
    const roundtrip = convertMetadata(result, schema)
    expect(roundtrip).toEqual(1234567890123456789n)
  })

  it('should correctly encode a Date as TIMESTAMP_MILLIS for INT64', () => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', type: 'INT64', converted_type: 'TIMESTAMP_MILLIS' }
    const date = new Date('2023-01-01T00:00:00Z')
    const result = unconvertMetadata(date, schema)
    const roundtrip = convertMetadata(result, schema)
    expect(roundtrip).toEqual(date)
  })

  it('should throw an error for unsupported types', () => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', type: 'INT96' }
    expect(() => unconvertMetadata(123, schema))
      .toThrow('unsupported type for statistics: INT96 with value 123')
  })

  it('should throw an error for INT64 if value is a number instead of bigint or Date', () => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', type: 'INT64' }
    expect(() => unconvertMetadata(123, schema))
      .toThrow('unsupported type for statistics: INT64 with value 123')
  })
})
