import { describe, expect, it } from 'vitest'
import { ByteWriter } from '../src/bytewriter.js'
import { writeByteStreamSplit } from '../src/splitstream.js'
import { byteStreamSplit } from 'hyparquet/src/encoding.js'

/**
 * @import {DecodedArray, ParquetType} from 'hyparquet'
 * @param {DecodedArray} values
 * @param {ParquetType} type
 * @param {number} [typeLength]
 * @returns {DecodedArray}
 */
function roundTrip(values, type, typeLength) {
  const writer = new ByteWriter()
  writeByteStreamSplit(writer, values, type, typeLength)
  const buffer = writer.getBuffer()
  const reader = { view: new DataView(buffer), offset: 0 }
  return byteStreamSplit(reader, values.length, type, typeLength)
}

describe('BYTE_STREAM_SPLIT encoding', () => {
  describe('FLOAT', () => {
    it('should round-trip float values', () => {
      const original = [1.5, 2.25, 3.125, -4.5, 0.0, 100.75]
      expect(Array.from(roundTrip(original, 'FLOAT'))).toEqual(original)
    })

    it('should round-trip an empty array', () => {
      expect(Array.from(roundTrip([], 'FLOAT'))).toEqual([])
    })

    it('should round-trip special float values', () => {
      const decoded = roundTrip([0.0, -0.0, Infinity, -Infinity], 'FLOAT')
      expect(decoded[0]).toBe(0.0)
      expect(decoded[1]).toBe(-0.0)
      expect(decoded[2]).toBe(Infinity)
      expect(decoded[3]).toBe(-Infinity)
    })
  })

  describe('DOUBLE', () => {
    it('should round-trip double values', () => {
      const original = [1.5, 2.25, 3.125, -4.5, 0.0, 100.75, 1e100, -1e-100]
      expect(Array.from(roundTrip(original, 'DOUBLE'))).toEqual(original)
    })

    it('should round-trip an empty array', () => {
      expect(Array.from(roundTrip([], 'DOUBLE'))).toEqual([])
    })
  })

  describe('INT32', () => {
    it('should round-trip int32 values', () => {
      const original = [1, 2, 3, -100, 0, 2147483647, -2147483648]
      expect(Array.from(roundTrip(original, 'INT32'))).toEqual(original)
    })

    it('should round-trip an empty array', () => {
      expect(Array.from(roundTrip([], 'INT32'))).toEqual([])
    })
  })

  describe('INT64', () => {
    it('should round-trip int64 values', () => {
      const original = [1n, 2n, 3n, -100n, 0n, 9223372036854775807n, -9223372036854775808n]
      expect(Array.from(roundTrip(original, 'INT64'))).toEqual(original)
    })

    it('should round-trip an empty array', () => {
      expect(Array.from(roundTrip([], 'INT64'))).toEqual([])
    })
  })

  describe('FIXED_LEN_BYTE_ARRAY', () => {
    it('should round-trip fixed-length byte arrays', () => {
      const original = [
        new Uint8Array([1, 2, 3, 4]),
        new Uint8Array([5, 6, 7, 8]),
        new Uint8Array([9, 10, 11, 12]),
      ]
      const decoded = roundTrip(original, 'FIXED_LEN_BYTE_ARRAY', 4)
      expect(decoded).toHaveLength(3)
      expect(Array.from(decoded[0])).toEqual([1, 2, 3, 4])
      expect(Array.from(decoded[1])).toEqual([5, 6, 7, 8])
      expect(Array.from(decoded[2])).toEqual([9, 10, 11, 12])
    })

    it('should round-trip an empty array', () => {
      const decoded = roundTrip([], 'FIXED_LEN_BYTE_ARRAY', 4)
      expect(Array.from(decoded)).toEqual([])
    })

    it('should throw without typeLength', () => {
      const writer = new ByteWriter()
      expect(() => writeByteStreamSplit(writer, [], 'FIXED_LEN_BYTE_ARRAY', undefined))
        .toThrow('missing type_length')
    })
  })

  describe('errors', () => {
    it('should throw for unsupported type', () => {
      const writer = new ByteWriter()
      expect(() => writeByteStreamSplit(writer, [], 'BOOLEAN', undefined))
        .toThrow('unsupported type')
    })
  })
})
