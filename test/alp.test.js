import { describe, expect, it } from 'vitest'
import { ByteWriter } from '../src/bytewriter.js'
import { writeALP } from '../src/alp.js'
// Use local hyparquet repo until ALP is released
import { alpDecode } from '../../hyparquet/src/alp.js'

/**
 * Round-trip test for ALP encoding with Float32Array output.
 *
 * @param {number[]} values
 * @returns {number[]}
 */
function roundTripFloat(values) {
  const writer = new ByteWriter()
  writeALP(writer, values, 'FLOAT')
  const buffer = writer.getBuffer()
  const reader = { view: new DataView(buffer), offset: 0 }

  const output = alpDecode(reader, values.length, 'FLOAT')
  return Array.from(output)
}

/**
 * Round-trip test for ALP encoding with Float64Array output.
 *
 * @param {number[]} values
 * @returns {number[]}
 */
function roundTripDouble(values) {
  const writer = new ByteWriter()
  writeALP(writer, values, 'DOUBLE')
  const buffer = writer.getBuffer()
  const reader = { view: new DataView(buffer), offset: 0 }

  const output = alpDecode(reader, values.length, 'DOUBLE')
  return Array.from(output)
}

describe('writeALP float', () => {
  it('should round-trip empty array', () => {
    const decoded = roundTripFloat([])
    expect(decoded).toEqual([])
  })

  it('should round-trip single value', () => {
    const decoded = roundTripFloat([1.5])
    expect(decoded).toEqual([1.5])
  })

  it('should round-trip simple decimal values', () => {
    const original = [1.23, 4.56, 7.89, 0.12]
    const decoded = roundTripFloat(original)
    expect(decoded.map(v => Math.fround(v))).toEqual(original.map(v => Math.fround(v)))
  })

  it('should round-trip all identical values (bit_width = 0)', () => {
    const original = Array(100).fill(42.5)
    const decoded = roundTripFloat(original)
    expect(decoded).toEqual(original)
  })

  it('should round-trip monetary data', () => {
    const original = [19.99, 5.49, 149.00, 0.99, 299.99]
    const decoded = roundTripFloat(original)
    expect(decoded.map(v => Math.fround(v))).toEqual(original.map(v => Math.fround(v)))
  })

  it('should round-trip values with NaN exception', () => {
    const original = [1.5, NaN, 2.5]
    const decoded = roundTripFloat(original)
    expect(decoded[0]).toBe(1.5)
    expect(Number.isNaN(decoded[1])).toBe(true)
    expect(decoded[2]).toBe(2.5)
  })

  it('should round-trip values with Infinity exception', () => {
    const original = [1.0, Infinity, 2.0]
    const decoded = roundTripFloat(original)
    expect(decoded[0]).toBe(1.0)
    expect(decoded[1]).toBe(Infinity)
    expect(decoded[2]).toBe(2.0)
  })

  it('should round-trip values with -Infinity exception', () => {
    const original = [1.0, -Infinity, 2.0]
    const decoded = roundTripFloat(original)
    expect(decoded[0]).toBe(1.0)
    expect(decoded[1]).toBe(-Infinity)
    expect(decoded[2]).toBe(2.0)
  })

  it('should round-trip values with negative zero exception', () => {
    const original = [1.0, -0.0, 2.0]
    const decoded = roundTripFloat(original)
    expect(decoded[0]).toBe(1.0)
    expect(Object.is(decoded[1], -0)).toBe(true)
    expect(decoded[2]).toBe(2.0)
  })

  it('should round-trip values with round-trip failure exception', () => {
    // 0.333... will likely be an exception
    const original = [1.5, 1 / 3, 2.5]
    const decoded = roundTripFloat(original)
    expect(decoded[0]).toBe(1.5)
    expect(Math.fround(decoded[1])).toBe(Math.fround(1 / 3))
    expect(decoded[2]).toBe(2.5)
  })

  it('should round-trip integer values', () => {
    const original = [1, 2, 3, 4, 5]
    const decoded = roundTripFloat(original)
    expect(decoded).toEqual([1, 2, 3, 4, 5])
  })

  it('should round-trip negative values', () => {
    const original = [-1.5, -2.5, -3.5]
    const decoded = roundTripFloat(original)
    expect(decoded).toEqual([-1.5, -2.5, -3.5])
  })

  it('should round-trip mixed positive and negative values', () => {
    const original = [-10.5, 5.25, -2.75, 8.0]
    const decoded = roundTripFloat(original)
    expect(decoded.map(v => Math.fround(v))).toEqual(original.map(v => Math.fround(v)))
  })

  it('should throw for unsupported type', () => {
    const writer = new ByteWriter()
    expect(() => writeALP(writer, [1, 2, 3], 'INT32')).toThrow('ALP encoding unsupported type: INT32')
  })
})

describe('writeALP double', () => {
  it('should round-trip empty array', () => {
    const decoded = roundTripDouble([])
    expect(decoded).toEqual([])
  })

  it('should round-trip single value', () => {
    const decoded = roundTripDouble([1.5])
    expect(decoded).toEqual([1.5])
  })

  it('should round-trip simple decimal values', () => {
    const original = [1.23, 4.56, 7.89, 0.12]
    const decoded = roundTripDouble(original)
    expect(decoded).toEqual(original)
  })

  it('should round-trip all identical values (bit_width = 0)', () => {
    const original = Array(100).fill(42.5)
    const decoded = roundTripDouble(original)
    expect(decoded).toEqual(original)
  })

  it('should round-trip monetary data', () => {
    const original = [19.99, 5.49, 149.00, 0.99, 299.99]
    const decoded = roundTripDouble(original)
    expect(decoded).toEqual(original)
  })

  it('should round-trip values with NaN exception', () => {
    const original = [1.5, NaN, 2.5]
    const decoded = roundTripDouble(original)
    expect(decoded[0]).toBe(1.5)
    expect(Number.isNaN(decoded[1])).toBe(true)
    expect(decoded[2]).toBe(2.5)
  })

  it('should round-trip values with Infinity exception', () => {
    const original = [1.0, Infinity, 2.0]
    const decoded = roundTripDouble(original)
    expect(decoded[0]).toBe(1.0)
    expect(decoded[1]).toBe(Infinity)
    expect(decoded[2]).toBe(2.0)
  })

  it('should round-trip values with -Infinity exception', () => {
    const original = [1.0, -Infinity, 2.0]
    const decoded = roundTripDouble(original)
    expect(decoded[0]).toBe(1.0)
    expect(decoded[1]).toBe(-Infinity)
    expect(decoded[2]).toBe(2.0)
  })

  it('should round-trip values with negative zero exception', () => {
    const original = [1.0, -0.0, 2.0]
    const decoded = roundTripDouble(original)
    expect(decoded[0]).toBe(1.0)
    expect(Object.is(decoded[1], -0)).toBe(true)
    expect(decoded[2]).toBe(2.0)
  })

  it('should round-trip integer values', () => {
    const original = [1, 2, 3, 4, 5]
    const decoded = roundTripDouble(original)
    expect(decoded).toEqual([1, 2, 3, 4, 5])
  })

  it('should round-trip negative values', () => {
    const original = [-1.5, -2.5, -3.5]
    const decoded = roundTripDouble(original)
    expect(decoded).toEqual([-1.5, -2.5, -3.5])
  })

  it('should round-trip high precision values', () => {
    const original = [3.141592653589793, 2.718281828459045, 1.4142135623730951]
    const decoded = roundTripDouble(original)
    expect(decoded).toEqual(original)
  })

  it('should round-trip large values', () => {
    const original = [1e15, 2e15, 3e15]
    const decoded = roundTripDouble(original)
    expect(decoded).toEqual(original)
  })
})

describe('writeALP multiple vectors', () => {
  it('should round-trip more than 1024 float values', () => {
    const original = Array.from({ length: 2000 }, (_, i) => i * 0.01)
    const decoded = roundTripFloat(original)
    expect(decoded.length).toBe(original.length)
    for (let i = 0; i < original.length; i++) {
      expect(Math.fround(decoded[i])).toBe(Math.fround(original[i]))
    }
  })

  it('should round-trip more than 1024 double values', () => {
    const original = Array.from({ length: 2000 }, (_, i) => i * 0.01)
    const decoded = roundTripDouble(original)
    expect(decoded.length).toBe(original.length)
    for (let i = 0; i < original.length; i++) {
      expect(decoded[i]).toBe(original[i])
    }
  })

  it('should round-trip exactly 1024 values (one full vector)', () => {
    const original = Array.from({ length: 1024 }, (_, i) => i * 0.1)
    const decoded = roundTripDouble(original)
    expect(decoded.length).toBe(original.length)
    expect(decoded).toEqual(original)
  })

  it('should round-trip 1025 values (two vectors)', () => {
    const original = Array.from({ length: 1025 }, (_, i) => i * 0.1)
    const decoded = roundTripDouble(original)
    expect(decoded.length).toBe(original.length)
    expect(decoded).toEqual(original)
  })
})

describe('writeALP edge cases', () => {
  it('should handle all exceptions', () => {
    const original = [NaN, Infinity, -Infinity, -0.0]
    const decoded = roundTripDouble(original)
    expect(Number.isNaN(decoded[0])).toBe(true)
    expect(decoded[1]).toBe(Infinity)
    expect(decoded[2]).toBe(-Infinity)
    expect(Object.is(decoded[3], -0)).toBe(true)
  })

  it('should handle zero values', () => {
    const original = [0, 0, 0, 0]
    const decoded = roundTripDouble(original)
    expect(decoded).toEqual([0, 0, 0, 0])
  })

  it('should handle very small values', () => {
    const original = [0.001, 0.002, 0.003]
    const decoded = roundTripDouble(original)
    expect(decoded).toEqual(original)
  })
})
