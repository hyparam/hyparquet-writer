import { deserializeTCompactProtocol } from 'hyparquet/src/thrift.js'
import { describe, expect, it } from 'vitest'
import { serializeTCompactProtocol } from '../src/thrift.js'
import { Writer } from '../src/writer.js'

/**
 * Utility to decode a Thrift-serialized buffer and return the parsed object.
 * @param {ArrayBuffer} buf
 * @returns {Record<string, any>}
 */
function roundTripDeserialize(buf) {
  const view = new DataView(buf)
  const reader = { view, offset: 0 }
  return deserializeTCompactProtocol(reader)
}

describe('serializeTCompactProtocol', () => {
  it('serializes basic types correctly', () => {
    const data = {
      field_1: true, // BOOL -> TRUE
      field_2: false, // BOOL -> FALSE
      field_3: 127, // BYTE / I32
      field_4: 0x7fff, // I16 / I32
      field_5: 0x7fffffff, // I32
      field_6: BigInt('0x7fffffffffffffff'), // I64
      field_7: 123.456, // DOUBLE
      field_8: 'Hello, Thrift!',
      field_9: new TextEncoder().encode('Hello, Thrift!'),
    }

    const writer = new Writer()
    serializeTCompactProtocol(writer, data)
    const buf = writer.buffer.slice(0, writer.offset)
    const result = roundTripDeserialize(buf)

    expect(result.field_1).toBe(true)
    expect(result.field_2).toBe(false)
    expect(result.field_3).toBe(127)
    expect(result.field_4).toBe(0x7fff)
    expect(result.field_5).toBe(0x7fffffff)
    expect(result.field_6).toBe(BigInt('0x7fffffffffffffff'))
    expect(result.field_7).toBeCloseTo(123.456)
    // Decode the binary back into a string
    const decoder = new TextDecoder()
    expect(decoder.decode(result.field_8)).toBe('Hello, Thrift!')
    expect(decoder.decode(result.field_9)).toBe('Hello, Thrift!')
  })

  it('serializes a nested STRUCT and LIST of booleans', () => {
    const data = {
      field_1: {
        field_1: 42,
        field_2: {
          field_1: true,
          field_2: false,
        },
      },
      // List of booleans
      field_2: [true, false, true, false],
    }

    const writer = new Writer()
    serializeTCompactProtocol(writer, data)
    const buf = writer.buffer.slice(0, writer.offset)
    const result = roundTripDeserialize(buf)

    expect(result.field_1.field_1).toBe(42)
    expect(result.field_1.field_2.field_1).toBe(true)
    expect(result.field_1.field_2.field_2).toBe(false)
    expect(result.field_2).toEqual([true, false, true, false])
  })

  it('handles empty object (only STOP)', () => {
    const data = {}
    const writer = new Writer()
    serializeTCompactProtocol(writer, data)
    const buf = writer.buffer.slice(0, writer.offset)
    const arr = new Uint8Array(buf)
    // The entire buffer should just be [0x00] = STOP
    expect(arr).toEqual(new Uint8Array([0x00]))

    // Round-trip: should deserialize to an empty object
    const result = roundTripDeserialize(buf)
    expect(result).toEqual({})
  })

  it('throws on non-monotonic field IDs', () => {
    const invalidData = {
      field_2: 2,
      field_1: 1, // field_1 is out of order (less than field_2)
    }
    const writer = new Writer()
    expect(() => serializeTCompactProtocol(writer, invalidData)).toThrow()
  })
})
