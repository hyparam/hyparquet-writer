import { parquetMetadata, parquetReadObjects } from 'hyparquet'
import { describe, expect, it } from 'vitest'
import { parquetWrite } from '../src/index.js'
import { exampleMetadata } from './metadata.test.js'

/**
 * Utility to encode a parquet file and then read it back into a JS object.
 *
 * @param {Record<string, any[]>} columnData
 * @returns {Promise<Record<string, any>>}
 */
async function roundTripDeserialize(columnData) {
  const file = parquetWrite(columnData)
  return await parquetReadObjects({ file })
}

const data = {
  bool: [true, false, true, false], // BOOLEAN
  int: [0, 127, 0x7fff, 0x7fffffff], // INT32
  bigint: [0n, 127n, 0x7fffn, 0x7fffffffffffffffn], // INT64
  double: [0, 0.0001, 123.456, 1e100], // DOUBLE
  string: ['a', 'b', 'c', 'd'], // BYTE_ARRAY
  nullable: [true, false, null, null], // BOOLEAN nullable
}

describe('parquetWrite', () => {
  it('writes expected metadata', () => {
    const file = parquetWrite(data)
    const metadata = parquetMetadata(file)
    expect(metadata).toEqual(exampleMetadata)
  })

  it('serializes basic types', async () => {
    const result = await roundTripDeserialize(data)
    expect(result).toEqual([
      { bool: true, int: 0, bigint: 0n, double: 0, string: 'a', nullable: true },
      { bool: false, int: 127, bigint: 127n, double: 0.0001, string: 'b', nullable: false },
      { bool: true, int: 0x7fff, bigint: 0x7fffn, double: 123.456, string: 'c', nullable: null },
      { bool: false, int: 0x7fffffff, bigint: 0x7fffffffffffffffn, double: 1e100, string: 'd', nullable: null },
    ])
  })

  it('efficiently serializes sparse booleans', () => {
    const bool = Array(10000).fill(null)
    bool[10] = true
    bool[100] = false
    bool[500] = true
    bool[9999] = false
    const buffer = parquetWrite({ bool })
    expect(buffer.byteLength).toBe(1399)
    const metadata = parquetMetadata(buffer)
    expect(metadata.metadata_length).toBe(89)
  })

  it('serializes list types', async () => {
    const result = await roundTripDeserialize({
      list: [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]],
    })
    expect(result).toEqual([
      { list: [1, 2, 3] },
      { list: [4, 5, 6] },
      { list: [7, 8, 9] },
      { list: [10, 11, 12] },
    ])
  })

  it('serializes object types', async () => {
    const result = await roundTripDeserialize({
      obj: [{ a: 1, b: 2 }, { a: 3, b: 4 }, { a: 5, b: 6 }, { a: 7, b: 8 }],
    })
    expect(result).toEqual([
      { obj: { a: 1, b: 2 } },
      { obj: { a: 3, b: 4 } },
      { obj: { a: 5, b: 6 } },
      { obj: { a: 7, b: 8 } },
    ])
  })

  it('serializes date types', async () => {
    const result = await roundTripDeserialize({
      date: [new Date(0), new Date(100000), new Date(200000), new Date(300000)],
    })
    expect(result).toEqual([
      { date: new Date(0) },
      { date: new Date(100000) },
      { date: new Date(200000) },
      { date: new Date(300000) },
    ])
  })

  it('throws for mixed types', () => {
    expect(() => parquetWrite({ mixed: [1, 2, 3, 'boom'] }))
      .toThrow('parquet cannot write mixed types: INT32 and BYTE_ARRAY')
  })
})
