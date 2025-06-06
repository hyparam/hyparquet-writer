import { describe, expect, it } from 'vitest'
import { ByteWriter } from '../src/bytewriter.js'
import { writePlain } from '../src/plain.js'

describe('writePlain', () => {
  it('writes BOOLEAN (multiple of 8 bits, plus leftover)', () => {
    const writer = new ByteWriter()
    const booleans = [true, false, true, true, false, false, false, true, true]
    writePlain(writer, booleans, 'BOOLEAN', undefined)

    expect(writer.offset).toBe(2)
    expect(writer.view.getUint8(0)).toBe(0b10001101)
    expect(writer.view.getUint8(1)).toBe(0b00000001)
  })

  it('writes INT32', () => {
    const writer = new ByteWriter()
    const ints = [0, 1, 255, 256, 65535, -1, -2147483648, 2147483647]
    writePlain(writer, ints, 'INT32', undefined)

    // 4 bytes per int
    expect(writer.offset).toBe(4 * ints.length)

    for (let i = 0; i < ints.length; i++) {
      const value = writer.view.getInt32(i * 4, true)
      expect(value).toBe(ints[i])
    }
  })

  it('writes INT64', () => {
    const writer = new ByteWriter()
    const bigints = [0n, 1n, 42n, BigInt(2 ** 53 - 1)]
    writePlain(writer, bigints, 'INT64', undefined)

    // 8 bytes per int64
    expect(writer.offset).toBe(8 * bigints.length)

    for (let i = 0; i < bigints.length; i++) {
      const value = writer.view.getBigInt64(i * 8, true)
      expect(value).toBe(bigints[i])
    }
  })

  it('writes FLOAT', () => {
    const writer = new ByteWriter()
    const floats = [0, 300.5, -2.7100000381469727, Infinity, -Infinity, NaN]
    writePlain(writer, floats, 'FLOAT', undefined)

    // 4 bytes per float
    expect(writer.offset).toBe(4 * floats.length)

    for (let i = 0; i < floats.length; i++) {
      const val = writer.view.getFloat32(i * 4, true)
      if (Number.isNaN(floats[i])) {
        expect(Number.isNaN(val)).toBe(true)
      } else {
        expect(val).toBe(floats[i])
      }
    }
  })

  it('writes DOUBLE', () => {
    const writer = new ByteWriter()
    const doubles = [0, 3.14, -2.71, Infinity, -Infinity, NaN]
    writePlain(writer, doubles, 'DOUBLE', undefined)

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

  it('writes BYTE_ARRAY', () => {
    const writer = new ByteWriter()
    const strings = ['a', 'b', 'c', 'd']
    writePlain(writer, strings, 'BYTE_ARRAY', undefined)

    let offset = 0
    for (const s of strings) {
      const length = writer.view.getUint32(offset, true)
      expect(length).toBe(s.length)
      offset += 4

      for (let i = 0; i < s.length; i++) {
        expect(writer.view.getUint8(offset)).toBe(s.charCodeAt(i))
        offset += 1
      }
    }
  })

  it('writes FIXED_LENGTH_BYTE_ARRAY', () => {
    const writer = new ByteWriter()
    const encoder = new TextEncoder()
    const strings = ['abcd', 'efgh', 'ijkl']
      .map(s => encoder.encode(s))
    writePlain(writer, strings, 'FIXED_LEN_BYTE_ARRAY', 4)

    let offset = 0
    for (const s of strings) {
      for (let i = 0; i < s.length; i++) {
        expect(writer.view.getUint8(offset)).toBe(s[i])
        offset += 1
      }
    }
  })

  it('throws error on unsupported type', () => {
    const writer = new ByteWriter()
    expect(() => writePlain(writer, [1, 2, 3], 'INT96', undefined))
      .toThrow(/parquet unsupported type/i)
  })

  it('throws error on type mismatch', () => {
    const writer = new ByteWriter()
    expect(() => writePlain(writer, [1, 2, 3], 'BOOLEAN', undefined))
      .toThrow('parquet expected boolean value')
    expect(() => writePlain(writer, [1, 2, 3.5], 'INT32', undefined))
      .toThrow('parquet expected integer value')
    expect(() => writePlain(writer, [1n, 2n, 3], 'INT64', undefined))
      .toThrow('parquet expected bigint value')
    expect(() => writePlain(writer, [1, 2, 3n], 'FLOAT', undefined))
      .toThrow('parquet expected number value')
    expect(() => writePlain(writer, [1, 2, 3n], 'DOUBLE', undefined))
      .toThrow('parquet expected number value')
    expect(() => writePlain(writer, [1, 2, 3], 'BYTE_ARRAY', undefined))
      .toThrow('parquet expected Uint8Array value')
    expect(() => writePlain(writer, [1, 2, 3], 'FIXED_LEN_BYTE_ARRAY', undefined))
      .toThrow('parquet FIXED_LEN_BYTE_ARRAY expected type_length')
    expect(() => writePlain(writer, [1, 2, 3], 'FIXED_LEN_BYTE_ARRAY', 16))
      .toThrow('parquet expected Uint8Array value')
  })
})
