import { describe, expect, it } from 'vitest'
import { ByteWriter } from '../src/bytewriter.js'
import { deltaBinaryPack, deltaByteArray, deltaLengthByteArray } from '../src/delta.js'
import { deltaBinaryUnpack, deltaByteArray as deltaByteArrayRead, deltaLengthByteArray as deltaLengthByteArrayRead } from 'hyparquet/src/delta.js'

const decoder = new TextDecoder()
const encoder = new TextEncoder()

/**
 * Round-trip test for deltaBinaryPack with Int32Array output.
 *
 * @param {number[]} values
 * @returns {number[]}
 */
function roundTripInt32(values) {
  const writer = new ByteWriter()
  deltaBinaryPack(writer, values)
  const reader = { view: writer.view, offset: 0 }
  const output = new Int32Array(values.length)
  deltaBinaryUnpack(reader, values.length, output)
  return Array.from(output)
}

/**
 * Round-trip test for deltaBinaryPack with BigInt64Array output.
 *
 * @param {bigint[]} values
 * @returns {bigint[]}
 */
function roundTripBigInt(values) {
  const writer = new ByteWriter()
  deltaBinaryPack(writer, values)
  const reader = { view: writer.view, offset: 0 }
  const output = new BigInt64Array(values.length)
  deltaBinaryUnpack(reader, values.length, output)
  return Array.from(output)
}

/**
 * Round-trip test for deltaLengthByteArray.
 *
 * @param {Uint8Array[]} values
 * @returns {Uint8Array[]}
 */
function roundTripLengthByteArray(values) {
  const writer = new ByteWriter()
  deltaLengthByteArray(writer, values)
  const reader = { view: writer.view, offset: 0 }
  /** @type {Uint8Array[]} */
  const output = new Array(values.length)
  deltaLengthByteArrayRead(reader, values.length, output)
  return output
}

/**
 * Round-trip test for deltaByteArray.
 *
 * @param {Uint8Array[]} values
 * @returns {Uint8Array[]}
 */
function roundTripByteArray(values) {
  const writer = new ByteWriter()
  deltaByteArray(writer, values)
  const reader = { view: writer.view, offset: 0 }
  /** @type {Uint8Array[]} */
  const output = new Array(values.length)
  deltaByteArrayRead(reader, values.length, output)
  return output
}

describe('deltaBinaryPack', () => {
  it('should round-trip empty array', () => {
    const decoded = roundTripInt32([])
    expect(decoded).toEqual([])
  })

  it('should round-trip single value', () => {
    const decoded = roundTripInt32([42])
    expect(decoded).toEqual([42])
  })

  it('should round-trip monotonically increasing values', () => {
    const original = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    const decoded = roundTripInt32(original)
    expect(decoded).toEqual(original)
  })

  it('should round-trip constant values', () => {
    const original = Array(100).fill(42)
    const decoded = roundTripInt32(original)
    expect(decoded).toEqual(original)
  })

  it('should round-trip negative deltas', () => {
    const original = [100, 90, 80, 70, 60, 50, 40, 30, 20, 10]
    const decoded = roundTripInt32(original)
    expect(decoded).toEqual(original)
  })

  it('should round-trip mixed deltas', () => {
    const original = [0, 5, 3, 8, 2, 9, 1, 7, 4, 6]
    const decoded = roundTripInt32(original)
    expect(decoded).toEqual(original)
  })

  it('should round-trip values spanning multiple blocks', () => {
    // More than 128 values to test multiple blocks
    const original = Array.from({ length: 300 }, (_, i) => i * 2)
    const decoded = roundTripInt32(original)
    expect(decoded).toEqual(original)
  })

  it('should round-trip large values', () => {
    const original = [1000000, 1000001, 1000002, 1000003]
    const decoded = roundTripInt32(original)
    expect(decoded).toEqual(original)
  })

  it('should round-trip negative values', () => {
    const original = [-10, -5, 0, 5, 10]
    const decoded = roundTripInt32(original)
    expect(decoded).toEqual(original)
  })

  it('should round-trip bigint values', () => {
    const original = [1n, 2n, 3n, 4n, 5n]
    const decoded = roundTripBigInt(original)
    expect(decoded).toEqual(original)
  })

  it('should round-trip large bigint values', () => {
    const original = [10000000000n, 10000000001n, 10000000002n]
    const decoded = roundTripBigInt(original)
    expect(decoded).toEqual(original)
  })

  it('should round-trip random values', () => {
    const original = Array.from({ length: 200 }, () => Math.floor(Math.random() * 10000))
    const decoded = roundTripInt32(original)
    expect(decoded).toEqual(original)
  })

  it('should throw for unsupported types', () => {
    const writer = new ByteWriter()
    expect(() => deltaBinaryPack(writer, ['string'])).toThrow('deltaBinaryPack only supports number or bigint arrays')
  })

  it('should handle values requiring bit flush at end of miniblock', () => {
    // Values with varying bit widths to exercise the bitsUsed > 0 flush path
    const original = Array.from({ length: 32 }, (_, i) => i * 7)
    const decoded = roundTripInt32(original)
    expect(decoded).toEqual(original)
  })
})

describe('deltaLengthByteArray', () => {
  it('should round-trip empty array', () => {
    const decoded = roundTripLengthByteArray([])
    expect(decoded).toEqual([])
  })

  it('should round-trip single byte array', () => {
    const original = [new Uint8Array([1, 2, 3])]
    const decoded = roundTripLengthByteArray(original)
    expect(decoded.length).toBe(1)
    expect(Array.from(decoded[0])).toEqual([1, 2, 3])
  })

  it('should round-trip multiple byte arrays', () => {
    const original = [
      new Uint8Array([1, 2, 3]),
      new Uint8Array([4, 5]),
      new Uint8Array([6, 7, 8, 9]),
    ]
    const decoded = roundTripLengthByteArray(original)
    expect(decoded.length).toBe(3)
    expect(Array.from(decoded[0])).toEqual([1, 2, 3])
    expect(Array.from(decoded[1])).toEqual([4, 5])
    expect(Array.from(decoded[2])).toEqual([6, 7, 8, 9])
  })

  it('should round-trip strings as byte arrays', () => {
    const original = ['hello', 'world', 'test'].map(s => encoder.encode(s))
    const decoded = roundTripLengthByteArray(original)
    expect(decoded.map(d => decoder.decode(d))).toEqual(['hello', 'world', 'test'])
  })

  it('should throw for non-Uint8Array values', () => {
    const writer = new ByteWriter()
    expect(() => deltaLengthByteArray(writer, ['string'])).toThrow('deltaLengthByteArray expects Uint8Array values')
  })
})

describe('deltaByteArray', () => {
  it('should round-trip empty array', () => {
    const decoded = roundTripByteArray([])
    expect(decoded).toEqual([])
  })

  it('should round-trip single byte array', () => {
    const original = [new Uint8Array([1, 2, 3])]
    const decoded = roundTripByteArray(original)
    expect(decoded.length).toBe(1)
    expect(Array.from(decoded[0])).toEqual([1, 2, 3])
  })

  it('should round-trip arrays with common prefixes', () => {
    const original = ['prefix_a', 'prefix_b', 'prefix_c'].map(s => encoder.encode(s))
    const decoded = roundTripByteArray(original)
    expect(decoded.map(d => decoder.decode(d))).toEqual(['prefix_a', 'prefix_b', 'prefix_c'])
  })

  it('should round-trip arrays with no common prefix', () => {
    const original = ['abc', 'xyz', '123'].map(s => encoder.encode(s))
    const decoded = roundTripByteArray(original)
    expect(decoded.map(d => decoder.decode(d))).toEqual(['abc', 'xyz', '123'])
  })

  it('should round-trip sorted strings efficiently', () => {
    const original = ['apple', 'application', 'apply', 'banana', 'bandana'].map(s => encoder.encode(s))
    const decoded = roundTripByteArray(original)
    expect(decoded.map(d => decoder.decode(d))).toEqual(['apple', 'application', 'apply', 'banana', 'bandana'])
  })

  it('should throw for non-Uint8Array first value', () => {
    const writer = new ByteWriter()
    expect(() => deltaByteArray(writer, ['string'])).toThrow('deltaByteArray expects Uint8Array values')
  })

  it('should throw for non-Uint8Array subsequent value', () => {
    const writer = new ByteWriter()
    expect(() => deltaByteArray(writer, [new Uint8Array([1]), 'string'])).toThrow('deltaByteArray expects Uint8Array values')
  })
})
