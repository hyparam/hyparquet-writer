import { describe, expect, it } from 'vitest'
import { Writer } from '../src/writer.js'
import { writePlain } from '../src/plain.js'

describe('writePlain', () => {
  it('writes BOOLEAN (multiple of 8 bits, plus leftover)', () => {
    const writer = new Writer()
    const booleans = [true, false, true, true, false, false, false, true, true]
    writePlain(writer, booleans, 'BOOLEAN')

    expect(writer.offset).toBe(2)
    expect(writer.view.getUint8(0)).toBe(0b10001101)
    expect(writer.view.getUint8(1)).toBe(0b00000001)
  })

  it('writes INT32', () => {
    const writer = new Writer()
    const ints = [0, 1, 255, 256, 65535, -1, -2147483648, 2147483647]
    writePlain(writer, ints, 'INT32')

    // 4 bytes per int
    expect(writer.offset).toBe(4 * ints.length)

    for (let i = 0; i < ints.length; i++) {
      const value = writer.view.getInt32(i * 4, true)
      expect(value).toBe(ints[i])
    }
  })

  it('writes INT64', () => {
    const writer = new Writer()
    const bigints = [0n, 1n, 42n, BigInt(2 ** 53 - 1)]
    writePlain(writer, bigints, 'INT64')

    // 8 bytes per int64
    expect(writer.offset).toBe(8 * bigints.length)

    for (let i = 0; i < bigints.length; i++) {
      const value = writer.view.getBigInt64(i * 8, true)
      expect(value).toBe(bigints[i])
    }
  })

  it('writes DOUBLE', () => {
    const writer = new Writer()
    const doubles = [0, 3.14, -2.71, Infinity, -Infinity, NaN]
    writePlain(writer, doubles, 'DOUBLE')

    // 8 bytes per double
    expect(writer.offset).toBe(8 * doubles.length)

    for (let i = 0; i < doubles.length; i++) {
      const val = writer.view.getFloat64(i * 8, true)
      if (Number.isNaN(doubles[i])) {
        expect(Number.isNaN(val)).toBe(true)
      } else {
        expect(val).toBe(doubles[i])
      }
    }
  })

  it('throws error on unsupported type', () => {
    const writer = new Writer()
    expect(() => writePlain(writer, [1, 2, 3], 'BYTE_ARRAY'))
      .toThrow(/parquet unsupported type/i)
  })
})
