import { deserializeTCompactProtocol } from 'hyparquet/src/thrift.js'
import { describe, expect, it } from 'vitest'
import { ByteWriter } from '../src/bytewriter.js'
import { logicalType } from '../src/metadata.js'
import { serializeTCompactProtocol } from '../src/thrift.js'

const decoder = new TextDecoder()

/**
 * @param {Record<string, any>} value
 * @returns {Record<string, any>}
 */
function roundTripThrift(value) {
  const writer = new ByteWriter()
  serializeTCompactProtocol(writer, value)
  return deserializeTCompactProtocol({ view: writer.view, offset: 0 })
}

describe('serializeTCompactProtocol', () => {
  it('serializes basic types correctly', () => {
    const data = {
      field_1: true, // BOOL -> TRUE
      field_2: false, // BOOL -> FALSE
      field_3: 127, // I32
      field_4: 0x7fff, // I32
      field_5: 0x7fffffff, // I32
      field_6: BigInt('0x7fffffffffffffff'), // I64
      field_7: 123.456, // DOUBLE
      field_8: 'Hello, Thrift!',
      field_9: new TextEncoder().encode('Hello, Thrift!'),
    }
    const result = roundTripThrift(data)
    expect(result.field_1).toBe(true)
    expect(result.field_2).toBe(false)
    expect(result.field_3).toBe(127)
    expect(result.field_4).toBe(0x7fff)
    expect(result.field_5).toBe(0x7fffffff)
    expect(result.field_6).toBe(BigInt('0x7fffffffffffffff'))
    expect(result.field_7).toBe(123.456)
    // Decode the binary back into a string
    expect(decoder.decode(result.field_8)).toBe('Hello, Thrift!')
    expect(decoder.decode(result.field_9)).toBe('Hello, Thrift!')
  })

  it('serializes STRUCTs', () => {
    const data = {
      field_1: {
        field_1: 42,
        field_2: {
          field_1: true,
          field_2: false,
        },
      },
    }
    const result = roundTripThrift(data)
    expect(result.field_1.field_1).toBe(42)
    expect(result.field_1.field_2.field_1).toBe(true)
    expect(result.field_1.field_2.field_2).toBe(false)
  })

  it('handles empty object (only STOP)', () => {
    expect(roundTripThrift({})).toEqual({})

    // The entire buffer should just be [0x00] = STOP
    const writer = new ByteWriter()
    serializeTCompactProtocol(writer, {})
    expect(writer.getBytes()).toEqual(new Uint8Array([0x00]))
  })

  it('handles missing struct fields', () => {
    const data = {
      field_1: 42,
      field_3: 3.14,
      field_200: 1000n, // big gap
    }
    const result = roundTripThrift(data)
    expect(result.field_1).toBe(42)
    expect(result.field_2).toBeUndefined()
    expect(result.field_3).toBe(3.14)
    expect(result.field_200).toBe(1000n)
  })

  it('skips undefined field values', () => {
    expect(roundTripThrift({ field_1: 42, field_2: undefined }))
      .toEqual({ field_1: 42 })
  })

  it('serializes LISTs', () => {
    const data = {
      field_1: [true, false, true, false], // booleans are special
      field_2: [1, 2, 3], // list of integers
      field_3: [1, 2.5, 5], // list of floats (needs expansion)
      field_4: ['a', 'b', 'c'], // list of strings
    }
    const result = roundTripThrift(data)
    expect(result.field_1).toEqual([true, false, true, false])
    expect(result.field_2).toEqual([1, 2, 3])
    expect(result.field_3).toEqual([1, 2.5, 5])
    expect(decoder.decode(result.field_4[0])).toBe('a')
    expect(decoder.decode(result.field_4[1])).toBe('b')
    expect(decoder.decode(result.field_4[2])).toBe('c')
  })

  it('serializes empty list', () => {
    expect(roundTripThrift({ field_1: [] })).toEqual({ field_1: [] })
  })

  it('serializes field IDs with gaps larger than 15', () => {
    const data = { field_1: 1, field_17: 17 }
    const result = roundTripThrift(data)
    expect(result.field_1).toBe(1)
    expect(result.field_17).toBe(17)
  })

  it('serializes GEOMETRY logicalType struct with field_17', () => {
    const data = { field_1: logicalType({ type: 'GEOMETRY' }) }
    const result = roundTripThrift(data)
    expect(result.field_1.field_17).toEqual({})
  })

  it('throws on non-monotonic field IDs', () => {
    const invalidData = {
      field_2: 33,
      field_1: 33, // field_1 is out of order
    }
    expect(() => roundTripThrift(invalidData))
      .toThrow('thrift non-monotonic field id: fid=1, lastFid=2')
  })

  it('throws on non-numeric field IDs', () => {
    const invalidData = {
      field_1: 33,
      field_two: 33, // field_two does not match field_N pattern
    }
    expect(() => roundTripThrift(invalidData))
      .toThrow('thrift invalid field name: field_two. Expected "field_###"')
  })

  it('throws on non-field IDs', () => {
    const invalidData = {
      field_1: 33,
      not_a_field: 33, // not_a_field does not match field_N pattern
    }
    expect(() => roundTripThrift(invalidData))
      .toThrow('thrift invalid field name: not_a_field. Expected "field_###"')
  })

  it('throws on null field value', () => {
    expect(() => roundTripThrift({ field_1: null }))
      .toThrow('Cannot determine thrift compact type for: null')
  })

  it('throws on heterogeneous list', () => {
    expect(() => roundTripThrift({ field_1: [1, 'hello'] }))
      .toThrow('thrift invalid type')
  })
})
