import { parquetMetadata, parquetReadObjects } from 'hyparquet'
import { describe, expect, it } from 'vitest'
import { parquetWrite } from '../src/index.js'
import { exampleMetadata } from './metadata.test.js'

/**
 * Utility to encode a parquet file and then read it back into a JS object.
 *
 * @import {ColumnData} from '../src/types.js'
 * @param {ColumnData[]} columnData
 * @returns {Promise<Record<string, any>>}
 */
async function roundTripDeserialize(columnData) {
  const file = parquetWrite({ columnData })
  return await parquetReadObjects({ file, utf8: false })
}

const basicData = [
  { name: 'bool', data: [true, false, true, false] },
  { name: 'int', data: [0, 127, 0x7fff, 0x7fffffff] },
  { name: 'bigint', data: [0n, 127n, 0x7fffn, 0x7fffffffffffffffn] },
  { name: 'double', data: [0, 0.0001, 123.456, 1e100] },
  { name: 'string', data: ['a', 'b', 'c', 'd'] },
  { name: 'nullable', data: [true, false, null, null] },
]

describe('parquetWrite', () => {
  it('writes expected metadata', () => {
    const file = parquetWrite({ columnData: basicData })
    const metadata = parquetMetadata(file)
    expect(metadata).toEqual(exampleMetadata)
  })

  it('serializes basic types', async () => {
    const result = await roundTripDeserialize(basicData)
    expect(result).toEqual([
      { bool: true, int: 0, bigint: 0n, double: 0, string: 'a', nullable: true },
      { bool: false, int: 127, bigint: 127n, double: 0.0001, string: 'b', nullable: false },
      { bool: true, int: 0x7fff, bigint: 0x7fffn, double: 123.456, string: 'c', nullable: null },
      { bool: false, int: 0x7fffffff, bigint: 0x7fffffffffffffffn, double: 1e100, string: 'd', nullable: null },
    ])
  })

  it('efficiently serializes sparse booleans', async () => {
    const bool = Array(10000).fill(null)
    bool[10] = true
    bool[100] = false
    bool[500] = true
    bool[9999] = false
    const file = parquetWrite({ columnData: [{ name: 'bool', data: bool }] })
    expect(file.byteLength).toBe(148)
    const metadata = parquetMetadata(file)
    expect(metadata.metadata_length).toBe(86)
    const result = await parquetReadObjects({ file })
    expect(result.length).toBe(10000)
    expect(result[0]).toEqual({ bool: null })
    expect(result[9]).toEqual({ bool: null })
    expect(result[10]).toEqual({ bool: true })
    expect(result[100]).toEqual({ bool: false })
    expect(result[500]).toEqual({ bool: true })
    expect(result[9999]).toEqual({ bool: false })
  })

  it('efficiently serializes long string', () => {
    const str = 'a'.repeat(10000)
    const file = parquetWrite({ columnData: [{ name: 'string', data: [str] }] })
    expect(file.byteLength).toBe(606)
  })

  it('less efficiently serializes string without compression', () => {
    const str = 'a'.repeat(10000)
    const columnData = [{ name: 'string', data: [str] }]
    const file = parquetWrite({ columnData, compressed: false })
    expect(file.byteLength).toBe(10135)
  })

  it('efficiently serializes column with few distinct values', async () => {
    const data = Array(10000).fill('aaaa')
    const file = parquetWrite({ columnData: [{ name: 'string', data }] })
    expect(file.byteLength).toBe(161)
    // round trip
    const result = await parquetReadObjects({ file })
    expect(result.length).toBe(10000)
    expect(result[0]).toEqual({ string: 'aaaa' })
  })

  it('serializes list types', async () => {
    const result = await roundTripDeserialize([{
      name: 'list',
      data: [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]],
    }])
    expect(result).toEqual([
      { list: [1, 2, 3] },
      { list: [4, 5, 6] },
      { list: [7, 8, 9] },
      { list: [10, 11, 12] },
    ])
  })

  it('serializes object types', async () => {
    const result = await roundTripDeserialize([{
      name: 'obj',
      data: [{ a: 1, b: 2 }, { a: 3, b: 4 }, { a: 5, b: 6 }, { a: 7, b: 8 }],
    }])
    expect(result).toEqual([
      { obj: { a: 1, b: 2 } },
      { obj: { a: 3, b: 4 } },
      { obj: { a: 5, b: 6 } },
      { obj: { a: 7, b: 8 } },
    ])
  })

  it('serializes date types', async () => {
    const result = await roundTripDeserialize([{
      name: 'date',
      data: [new Date(0), new Date(100000), new Date(200000), new Date(300000)],
    }])
    expect(result).toEqual([
      { date: new Date(0) },
      { date: new Date(100000) },
      { date: new Date(200000) },
      { date: new Date(300000) },
    ])
  })

  it('serializes byte array types', async () => {
    const result = await roundTripDeserialize([{
      name: 'bytes',
      data: [Uint8Array.of(1, 2, 3), Uint8Array.of(4, 5, 6), Uint8Array.of(7, 8, 9), Uint8Array.of(10, 11, 12)],
    }])
    expect(result).toEqual([
      { bytes: Uint8Array.of(1, 2, 3) },
      { bytes: Uint8Array.of(4, 5, 6) },
      { bytes: Uint8Array.of(7, 8, 9) },
      { bytes: Uint8Array.of(10, 11, 12) },
    ])
  })

  it('throws for mixed types', () => {
    expect(() => parquetWrite({ columnData: [{ name: 'mixed', data: [1, 2, 3, 'boom'] }] }))
      .toThrow('mixed types not supported')
  })
})
