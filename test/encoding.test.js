import { describe, expect, it } from 'vitest'
import { ByteWriter } from '../src/bytewriter.js'
import { writeRleBitPackedHybrid } from '../src/encoding.js'
import { readRleBitPackedHybrid } from 'hyparquet/src/encoding.js'

/**
 * Round-trip serialize and deserialize the given values.
 *
 * @param {number[]} values
 * @returns {number[]}
 */
function roundTripDeserialize(values) {
  const bitWidth = Math.ceil(Math.log2(Math.max(...values) + 1))

  // Serialize the values using writeRleBitPackedHybrid
  const writer = new ByteWriter()
  writeRleBitPackedHybrid(writer, values)
  const buffer = writer.getBuffer()
  const reader = { view: new DataView(buffer), offset: 0 }

  // Decode the values using readRleBitPackedHybrid from hyparquet
  /** @type {number[]} */
  const output = new Array(values.length)
  readRleBitPackedHybrid(reader, bitWidth, values.length, output)
  return output
}

describe('RLE bit-packed hybrid', () => {
  it('should round-trip a typical array of values', () => {
    const original = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    const decoded = roundTripDeserialize(original)
    expect(decoded).toEqual(original)
  })

  it('should round-trip an empty array', () => {
    const decoded = roundTripDeserialize([])
    expect(decoded).toEqual([])
  })

  it('should round-trip an array of zeros', () => {
    const original = [0, 0, 0, 0, 0, 0, 0, 0]
    const decoded = roundTripDeserialize(original)
    expect(decoded).toEqual(original)
  })

  it('should round-trip an array with large numbers', () => {
    const original = [1023, 511, 255, 127, 63, 31, 15, 7]
    const decoded = roundTripDeserialize(original)
    expect(decoded).toEqual(original)
  })

  it('should round-trip a random array of values', () => {
    const original = Array.from({ length: 20 }, () =>
      Math.floor(Math.random() * 1000)
    )
    const decoded = roundTripDeserialize(original)
    expect(decoded).toEqual(original)
  })

  it('should round-trip a sparse array of booleans', () => {
    const original = Array(10000).fill(0)
    original[10] = 1
    original[100] = 1
    original[500] = 1
    original[9999] = 1
    const decoded = roundTripDeserialize(original)
    expect(decoded).toEqual(original)
  })
})
