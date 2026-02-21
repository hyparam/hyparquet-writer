import { describe, expect, it } from 'vitest'
import { unconvert, unconvertDecimal, unconvertFloat16, unconvertMinMax } from '../src/unconvert.js'
import { convertMetadata } from 'hyparquet/src/metadata.js'
import { DEFAULT_PARSERS, parseFloat16 } from 'hyparquet/src/convert.js'

/**
 * @import {SchemaElement, TimeUnit} from 'hyparquet'
 */
describe('unconvert', () => {
  it('should return Date objects when converted_type = DATE', () => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', converted_type: 'DATE' }
    const input = [new Date('2020-01-01T00:00:00Z'), new Date('2021-01-01T00:00:00Z')]
    const result = unconvert(schema, input)
    expect(result).toEqual([18262, 18628])
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

  it('should handle undefined values in JSON arrays', () => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', converted_type: 'JSON' }
    const input = [{ foo: 'bar' }, undefined, { hello: 'world' }]
    const result = unconvert(schema, input)

    expect(result).toHaveLength(3)
    expect(result[0]).toBeInstanceOf(Uint8Array)
    expect(result[1]).toBeUndefined()
    expect(result[2]).toBeInstanceOf(Uint8Array)
    expect(new TextDecoder().decode(result[0])).toEqual(JSON.stringify({ foo: 'bar' }))
    expect(new TextDecoder().decode(result[2])).toEqual(JSON.stringify({ hello: 'world' }))
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

  /** @type {Array<{name: string, schema: SchemaElement, input: any[], expected: any[]}>} */
  const convertedTypeConversionCases = [
    {
      name: 'DATE passthrough',
      schema: { name: 'test', converted_type: 'DATE' },
      input: [20503, 20504],
      expected: [20503, 20504],
    },
    {
      name: 'TIMESTAMP_MILLIS passthrough',
      schema: { name: 'test', converted_type: 'TIMESTAMP_MILLIS' },
      input: [1771544750000n, 1771631150000n],
      expected: [1771544750000n, 1771631150000n],
    },
    {
      name: 'TIMESTAMP_MILLIS from Date',
      schema: { name: 'test', converted_type: 'TIMESTAMP_MILLIS' },
      input: [new Date('2026-02-19T23:45:50Z')],
      expected: [1771544750000n],
    },
    {
      name: 'TIMESTAMP_MICROS passthrough',
      schema: { name: 'test', converted_type: 'TIMESTAMP_MICROS' },
      input: [1771544750000000n, 1771631150000000n],
      expected: [1771544750000000n, 1771631150000000n],
    },
    {
      name: 'TIMESTAMP_MICROS from Date',
      schema: { name: 'test', converted_type: 'TIMESTAMP_MICROS' },
      input: [new Date('2026-02-19T23:45:50Z')],
      expected: [1771544750000000n],
    },
  ]
  it.for(convertedTypeConversionCases)('should convert $name via converted_type', ({ schema, input, expected }) => {
    const result = unconvert(schema, input)
    expect(result).toEqual(expected)
  })

  it('should handle null values when converted_type = DATE', () => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', converted_type: 'DATE' }
    const input = [null, new Date('2026-02-19T00:00:00Z'), undefined]
    const result = unconvert(schema, input)
    expect(result).toEqual([null, 20503, undefined])
  })

  /** @type {Array<{unit: TimeUnit, expected: bigint[]}>} */
  const dateToTimestampCases = [
    {
      unit: 'MILLIS',
      expected: [1771544750000n],
    },
    {
      unit: 'MICROS',
      expected: [1771544750000000n],
    },
    {
      unit: 'NANOS',
      expected: [1771544750000000000n],
    },
  ]
  it.for(dateToTimestampCases)('should convert Date to bigint for logical_type TIMESTAMP $unit', ({ unit, expected }) => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', logical_type: { type: 'TIMESTAMP', isAdjustedToUTC: true, unit } }
    const input = [new Date('2026-02-19T23:45:50Z')]
    const result = unconvert(schema, input)
    expect(result).toEqual(expected)
  })

  /** @type {Array<{unit: TimeUnit, input: bigint[], expected: bigint[]}>} */
  const bigintPassthroughCases = [
    {
      unit: 'MILLIS',
      input: [1771544750000n],
      expected: [1771544750000n],
    },
    {
      unit: 'MICROS',
      input: [1771544750000000n],
      expected: [1771544750000000n],
    },
    {
      unit: 'NANOS',
      input: [1771544750000000000n],
      expected: [1771544750000000000n],
    },
  ]
  it.for(bigintPassthroughCases)('should pass through bigint values for logical_type TIMESTAMP $unit', ({ unit, input, expected }) => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', logical_type: { type: 'TIMESTAMP', isAdjustedToUTC: true, unit } }
    const result = unconvert(schema, input)
    expect(result).toEqual(expected)
  })

  /** @type {Array<{unit: TimeUnit, input: any[], expected: any[]}>} */
  const nullTimestampCases = [
    {
      unit: 'MILLIS',
      input: [null, 1771544750000n, undefined],
      expected: [null, 1771544750000n, undefined],
    },
    {
      unit: 'MICROS',
      input: [null, 1771544750000000n, undefined],
      expected: [null, 1771544750000000n, undefined],
    },
    {
      unit: 'NANOS',
      input: [null, 1771544750000000000n, undefined],
      expected: [null, 1771544750000000000n, undefined],
    },
  ]
  it.for(nullTimestampCases)('should handle null values in logical_type TIMESTAMP $unit', ({ unit, input, expected }) => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', logical_type: { type: 'TIMESTAMP', isAdjustedToUTC: true, unit } }
    const result = unconvert(schema, input)
    expect(result).toEqual(expected)
  })

  it('should return original values if there is no recognized converted_type', () => {
    const input = [1, 2, 3]
    const result = unconvert({ name: 'test' }, input)
    expect(result).toEqual(input)
  })
})

describe('unconvertMinMax', () => {
  it('should return undefined if value is undefined or null', () => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', type: 'INT32' }
    expect(unconvertMinMax(undefined, schema)).toBeUndefined()
  })

  it('should handle BOOLEAN type', () => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', type: 'BOOLEAN' }
    expect(unconvertMinMax(true, schema)).toEqual(new Uint8Array([1]))
    expect(unconvertMinMax(false, schema)).toEqual(new Uint8Array([0]))
  })

  it('should truncate BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY to 16 bytes', () => {
    // longer string to test truncation
    const longStr = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    const longStrUint8 = new TextEncoder().encode(longStr)

    // value is a Uint8Array
    const result1 = unconvertMinMax(longStrUint8, { name: 'test', type: 'BYTE_ARRAY' })
    expect(result1).toBeInstanceOf(Uint8Array)
    expect(result1?.length).toBe(16)

    // value is a string
    const result2 = unconvertMinMax(longStr, { name: 'test', type: 'FIXED_LEN_BYTE_ARRAY' })
    expect(result2).toBeInstanceOf(Uint8Array)
    expect(result2?.length).toBe(16)
  })

  it('should correctly encode FLOAT values in little-endian', () => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', type: 'FLOAT' }
    const value = 1.5
    const result = unconvertMinMax(value, schema)
    expect(result).toBeInstanceOf(Uint8Array)
    const roundtrip = convertMetadata(result, schema, DEFAULT_PARSERS)
    expect(roundtrip).toEqual(1.5)
  })

  it('should correctly encode DOUBLE values in little-endian', () => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', type: 'DOUBLE' }
    const value = 1.123456789
    const result = unconvertMinMax(value, schema)
    expect(result).toBeInstanceOf(Uint8Array)
    const roundtrip = convertMetadata(result, schema, DEFAULT_PARSERS)
    expect(roundtrip).toEqual(1.123456789)
  })

  it('should correctly encode INT32 values in little-endian', () => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', type: 'INT32' }
    const value = 123456
    const result = unconvertMinMax(value, schema)
    const roundtrip = convertMetadata(result, schema, DEFAULT_PARSERS)
    expect(roundtrip).toEqual(123456)
  })

  it('should correctly encode INT64 values when given a bigint', () => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', type: 'INT64' }
    const value = 1234567890123456789n
    const result = unconvertMinMax(value, schema)
    const roundtrip = convertMetadata(result, schema, DEFAULT_PARSERS)
    expect(roundtrip).toEqual(1234567890123456789n)
  })

  it('should correctly encode a Date as TIMESTAMP_MILLIS for INT64', () => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', type: 'INT64', converted_type: 'TIMESTAMP_MILLIS' }
    const date = new Date('2023-01-01T00:00:00Z')
    const result = unconvertMinMax(date, schema)
    const roundtrip = convertMetadata(result, schema, DEFAULT_PARSERS)
    expect(roundtrip).toEqual(date)
  })

  it('should throw an error for unsupported types', () => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', type: 'INT96' }
    expect(() => unconvertMinMax(123, schema))
      .toThrow('unsupported type for statistics: INT96 with value 123')
  })

  it('should throw an error for INT64 if value is a number instead of bigint or Date', () => {
    /** @type {SchemaElement} */
    const schema = { name: 'test', type: 'INT64' }
    expect(() => unconvertMinMax(123, schema))
      .toThrow('unsupported type for statistics: INT64 with value 123')
  })

  /** @type {Array<{name: string, schema: SchemaElement, value: bigint | Date, expected: bigint}>} */
  const int64EncodeCases = [
    {
      name: 'bigint TIMESTAMP_MILLIS',
      schema: { name: 'test', type: 'INT64', converted_type: 'TIMESTAMP_MILLIS' },
      value: 1771544750000n,
      expected: 1771544750000n,
    },
    {
      name: 'Date TIMESTAMP_MICROS',
      schema: { name: 'test', type: 'INT64', converted_type: 'TIMESTAMP_MICROS' },
      value: new Date('2026-02-19T23:45:50Z'),
      expected: 1771544750000000n,
    },
    {
      name: 'bigint TIMESTAMP_MICROS',
      schema: { name: 'test', type: 'INT64', converted_type: 'TIMESTAMP_MICROS' },
      value: 1771544750000000n,
      expected: 1771544750000000n,
    },
    {
      name: 'Date logical_type TIMESTAMP MILLIS',
      schema: { name: 'test', type: 'INT64', logical_type: { type: 'TIMESTAMP', isAdjustedToUTC: true, unit: 'MILLIS' } },
      value: new Date('2026-02-19T23:45:50Z'),
      expected: 1771544750000n,
    },
    {
      name: 'bigint logical_type TIMESTAMP MILLIS',
      schema: { name: 'test', type: 'INT64', logical_type: { type: 'TIMESTAMP', isAdjustedToUTC: true, unit: 'MILLIS' } },
      value: 1771544750000n,
      expected: 1771544750000n,
    },
    {
      name: 'Date logical_type TIMESTAMP MICROS',
      schema: { name: 'test', type: 'INT64', logical_type: { type: 'TIMESTAMP', isAdjustedToUTC: true, unit: 'MICROS' } },
      value: new Date('2026-02-19T23:45:50Z'),
      expected: 1771544750000000n,
    },
    {
      name: 'bigint logical_type TIMESTAMP MICROS',
      schema: { name: 'test', type: 'INT64', logical_type: { type: 'TIMESTAMP', isAdjustedToUTC: true, unit: 'MICROS' } },
      value: 1771544750000000n,
      expected: 1771544750000000n,
    },
    {
      name: 'Date logical_type TIMESTAMP NANOS',
      schema: { name: 'test', type: 'INT64', logical_type: { type: 'TIMESTAMP', isAdjustedToUTC: true, unit: 'NANOS' } },
      value: new Date('2026-02-19T23:45:50Z'),
      expected: 1771544750000000000n,
    },
    {
      name: 'bigint logical_type TIMESTAMP NANOS',
      schema: { name: 'test', type: 'INT64', logical_type: { type: 'TIMESTAMP', isAdjustedToUTC: true, unit: 'NANOS' } },
      value: 1771544750000000000n,
      expected: 1771544750000000000n,
    },
  ]
  it.for(int64EncodeCases)('should encode $name for INT64', ({ schema, value, expected }) => {
    const result = unconvertMinMax(value, schema)
    if (!result) throw new Error('expected result')
    const view = new DataView(result.buffer)
    expect(view.getBigInt64(0, true)).toEqual(expected)
  })
})

describe('unconvertDecimal', () => {
  const examples = [
    { input: 0n, expected: new Uint8Array([]) },
    { input: 1n, expected: new Uint8Array([0x01]) },
    { input: -1n, expected: new Uint8Array([0xff]) },
    { input: 1234n, expected: new Uint8Array([0x04, 0xd2]) },
    { input: -1234n, expected: new Uint8Array([0xfb, 0x2e]) },
    { input: 1234567890123456789n, expected: new Uint8Array([0x11, 0x22, 0x10, 0xf4, 0x7d, 0xe9, 0x81, 0x15]) },
    { input: -1234567890123456789n, expected: new Uint8Array([0xee, 0xdd, 0xef, 0x0b, 0x82, 0x16, 0x7e, 0xeb]) },
  ]
  /** @type {SchemaElement} */
  const element = {
    name: 'col',
    type: 'BYTE_ARRAY',
  }

  it.for(examples)('should convert %p', ({ input, expected }) => {
    expect(parseDecimal(expected)).toEqual(input)
  })

  it.for(examples)('should unconvert %p', ({ input, expected }) => {
    expect(unconvertDecimal(element, input)).toEqual(expected)
  })

  it.for(examples)('should roundtrip %p', ({ input }) => {
    const byteArray = unconvertDecimal(element, input)
    if (!(byteArray instanceof Uint8Array)) throw new Error('expected Uint8Array')
    expect(parseDecimal(byteArray)).toEqual(input)
  })

  it.for(examples)('should reverse roundtrip %p', ({ expected }) => {
    expect(unconvertDecimal(element, parseDecimal(expected))).toEqual(expected)
  })

  it('convert to INT32', () => {
    expect(unconvertDecimal({ name: 'col', type: 'INT32' }, 1234n)).toEqual(1234)
  })

  it('convert to INT64', () => {
    expect(unconvertDecimal({ name: 'col', type: 'INT64' }, 1234n)).toEqual(1234n)
  })

  it('throws if fixed length is not specified', () => {
    expect(() => unconvertDecimal({ name: 'col', type: 'FIXED_LEN_BYTE_ARRAY' }, 1234n))
      .toThrow('fixed length byte array type_length is required')
  })
})

describe('unconvertFloat16', () => {
  it('should convert number to Float16 array', () => {
    expect(unconvertFloat16(undefined)).toBeUndefined()
    expect(unconvertFloat16(0)).toEqual(new Uint8Array([0x00, 0x00]))
    expect(unconvertFloat16(-0)).toEqual(new Uint8Array([0x00, 0x80]))
    expect(unconvertFloat16(NaN)).toEqual(new Uint8Array([0x00, 0x7e]))
    expect(unconvertFloat16(Infinity)).toEqual(new Uint8Array([0x00, 0x7c]))
    expect(unconvertFloat16(-Infinity)).toEqual(new Uint8Array([0x00, 0xfc]))
    expect(unconvertFloat16(0.5)).toEqual(new Uint8Array([0x00, 0x38]))
    expect(unconvertFloat16(-0.5)).toEqual(new Uint8Array([0x00, 0xb8]))
    expect(unconvertFloat16(1)).toEqual(new Uint8Array([0x00, 0x3c]))
    expect(unconvertFloat16(-1)).toEqual(new Uint8Array([0x00, 0xbc]))
    expect(unconvertFloat16(0.000244140625)).toEqual(new Uint8Array([0x00, 0x0c]))
    // largest normal
    expect(unconvertFloat16(65504)).toEqual(new Uint8Array([0xff, 0x7b]))
    expect(unconvertFloat16(65505)).toEqual(new Uint8Array([0xff, 0x7b]))
    // subnormal
    expect(unconvertFloat16(2 ** -24)).toEqual(new Uint8Array([0x02, 0x00]))
    // mantissa overflow
    expect(unconvertFloat16(2047.9999)).toEqual(new Uint8Array([0x00, 0x68]))
  })

  it('should round-trip Float16', () => {
    expect(parseFloat16(unconvertFloat16(0))).toEqual(0)
    expect(parseFloat16(unconvertFloat16(-0))).toEqual(-0)
    expect(parseFloat16(unconvertFloat16(NaN))).toEqual(NaN)
    expect(parseFloat16(unconvertFloat16(Infinity))).toEqual(Infinity)
    expect(parseFloat16(unconvertFloat16(-Infinity))).toEqual(-Infinity)
    expect(parseFloat16(unconvertFloat16(0.5))).toEqual(0.5)
    expect(parseFloat16(unconvertFloat16(-0.5))).toEqual(-0.5)
    expect(parseFloat16(unconvertFloat16(1))).toEqual(1)
    expect(parseFloat16(unconvertFloat16(-1))).toEqual(-1)
    expect(parseFloat16(unconvertFloat16(65504))).toEqual(65504)
    expect(parseFloat16(unconvertFloat16(0.000244140625))).toEqual(0.000244140625)
  })
})

/**
 * BigInt parseDecimal
 * @param {Uint8Array} bytes
 * @returns {bigint}
 */
function parseDecimal(bytes) {
  let value = 0n
  for (const byte of bytes) {
    value = value * 256n + BigInt(byte)
  }

  // handle signed
  const bits = BigInt(bytes.length) * 8n
  if (bits && value >= 2n ** (bits - 1n)) {
    value -= 2n ** bits
  }

  return value
}
