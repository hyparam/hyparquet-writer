import { parquetMetadata, parquetReadObjects } from 'hyparquet'
import { describe, expect, it } from 'vitest'
import { parquetWriteBuffer } from '../src/index.js'
import { exampleData, exampleMetadata } from './example.js'

/**
 * Utility to encode a parquet file and then read it back into a JS object.
 *
 * @import {ColumnData} from '../src/types.js'
 * @param {ColumnData[]} columnData
 * @param {import('hyparquet').SchemaElement[]} [schema]
 * @returns {Promise<Record<string, any>>}
 */
async function roundTripDeserialize(columnData, schema) {
  const file = parquetWriteBuffer({ columnData, schema })
  return await parquetReadObjects({ file, utf8: false })
}

describe('parquetWriteBuffer', () => {
  it('writes expected metadata', () => {
    const file = parquetWriteBuffer({ columnData: exampleData })
    const metadata = parquetMetadata(file)
    expect(metadata).toEqual(exampleMetadata)
  })

  it('serializes basic types', async () => {
    const result = await roundTripDeserialize(exampleData)
    expect(result).toEqual([
      { bool: true, int: 0, bigint: 0n, float: 0, double: 0, string: 'a', nullable: true },
      { bool: false, int: 127, bigint: 127n, float: 0.00009999999747378752, double: 0.0001, string: 'b', nullable: false },
      { bool: true, int: 0x7fff, bigint: 0x7fffn, float: 123.45600128173828, double: 123.456, string: 'c', nullable: null },
      { bool: false, int: 0x7fffffff, bigint: 0x7fffffffffffffffn, float: Infinity, double: 1e100, string: 'd', nullable: null },
    ])
  })

  it('serializes a string as a BYTE_ARRAY', () => {
    const data = ['string1', 'string2', 'string3']
    const file = parquetWriteBuffer({ columnData: [{ name: 'string', data, type: 'BYTE_ARRAY' }] })
    expect(file.byteLength).toBe(164)
  })

  it('serializes booleans as RLE', async () => {
    const data = Array(100).fill(true)
    const file = parquetWriteBuffer({ columnData: [{ name: 'bool', data }] })
    expect(file.byteLength).toBe(131)
    const metadata = parquetMetadata(file)
    expect(metadata.row_groups[0].columns[0].meta_data?.encodings).toEqual(['RLE'])
    const result = await parquetReadObjects({ file })
    expect(result).toEqual(data.map(bool => ({ bool })))
  })

  it('efficiently serializes sparse booleans', async () => {
    const data = Array(10000).fill(null)
    data[10] = true
    data[100] = false
    data[500] = true
    data[9999] = false
    const file = parquetWriteBuffer({ columnData: [{ name: 'bool', data }] })
    expect(file.byteLength).toBe(159)
    const metadata = parquetMetadata(file)
    expect(metadata.metadata_length).toBe(92)
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
    const file = parquetWriteBuffer({ columnData: [{ name: 'string', data: [str] }] })
    expect(file.byteLength).toBe(638)
  })

  it('less efficiently serializes string without compression', () => {
    const str = 'a'.repeat(10000)
    const columnData = [{ name: 'string', data: [str] }]
    const file = parquetWriteBuffer({ columnData, compressed: false })
    expect(file.byteLength).toBe(10168)
  })

  it('efficiently serializes column with few distinct values', async () => {
    const data = Array(100000)
      .fill('aaaa', 0, 50000)
      .fill('bbbb', 50000, 100000)
    const file = parquetWriteBuffer({ columnData: [{ name: 'string', data }], statistics: false })
    expect(file.byteLength).toBe(170)
    // round trip
    const result = await parquetReadObjects({ file })
    expect(result.length).toBe(100000)
    expect(result[0]).toEqual({ string: 'aaaa' })
    expect(result[50000]).toEqual({ string: 'bbbb' })
  })

  it('writes statistics when enabled', () => {
    const withStats = parquetWriteBuffer({ columnData: exampleData, statistics: true })
    const noStats = parquetWriteBuffer({ columnData: exampleData, statistics: false })
    expect(withStats.byteLength).toBe(721)
    expect(noStats.byteLength).toBe(611)
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

  it('serializes time types', async () => {
    const result = await roundTripDeserialize(
      [
        {
          name: 'time32',
          data: [100000, 200000, 300000],
        },
        {
          name: 'time64',
          data: [100000000n, 200000000n, 300000000n],
        },
        {
          name: 'interval',
          data: [1000000000n, 2000000000n, 3000000000n],
        },
      ],
      [
        { name: 'root', num_children: 3 },
        { name: 'time32', repetition_type: 'OPTIONAL', type: 'INT32', logical_type: { type: 'TIME', isAdjustedToUTC: false, unit: 'MILLIS' } },
        { name: 'time64', repetition_type: 'OPTIONAL', type: 'INT64', logical_type: { type: 'TIME', isAdjustedToUTC: false, unit: 'MICROS' } },
        { name: 'interval', repetition_type: 'OPTIONAL', type: 'INT64', logical_type: { type: 'INTERVAL' } },
      ]
    )
    expect(result).toEqual([
      { time32: 100000, time64: 100000000n, interval: 1000000000n },
      { time32: 200000, time64: 200000000n, interval: 2000000000n },
      { time32: 300000, time64: 300000000n, interval: 3000000000n },
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

  it('serializes uuid types', async () => {
    const result = await roundTripDeserialize([
      {
        name: 'uuid',
        data: [
          new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
          new Uint8Array([17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32]),
        ],
        type: 'UUID',
      },
      {
        name: 'string',
        data: [
          '00000000-0000-0000-0000-000000000001',
          '00010002-0003-0004-0005-000600070008',
        ],
        type: 'UUID',
      },
    ])
    expect(result).toEqual([
      {
        uuid: new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
        string: new Uint8Array([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
      },
      {
        uuid: new Uint8Array([17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32]),
        string: new Uint8Array([0, 1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 6, 0, 7, 0, 8]),
      },
    ])
  })

  it('serializes empty column', async () => {
    const result = await roundTripDeserialize([{
      name: 'empty',
      data: [null, null, null, null],
      type: 'BOOLEAN',
    }])
    expect(result).toEqual([
      { empty: null },
      { empty: null },
      { empty: null },
      { empty: null },
    ])
  })

  it('serializes empty table', async () => {
    const result = await roundTripDeserialize([])
    expect(result).toEqual([])
  })

  it('handles special numeric values', async () => {
    const data = [
      { name: 'double', data: [NaN, Infinity, -Infinity, 42, 0, -0] },
    ]
    const result = await roundTripDeserialize(data)
    expect(result[0].double).toBeNaN()
    expect(result[1].double).toEqual(Infinity)
    expect(result[2].double).toEqual(-Infinity)
    expect(result[3].double).toEqual(42)
    expect(result[4].double).toEqual(0)
    expect(result[5].double).toEqual(-0)
    expect(result[5].double).not.toEqual(0)
  })

  it('splits row groups', async () => {
    const data = Array(200).fill(13)
    const file = parquetWriteBuffer({ columnData: [{ name: 'int', data }], rowGroupSize: 100 })
    const metadata = parquetMetadata(file)
    expect(metadata.row_groups.length).toBe(2)
    expect(metadata.row_groups[0].num_rows).toBe(100n)
    expect(metadata.row_groups[1].num_rows).toBe(100n)
    // round trip
    const result = await parquetReadObjects({ file })
    expect(result.length).toBe(200)
    expect(result[0]).toEqual({ int: 13 })
    expect(result[99]).toEqual({ int: 13 })
    expect(result[100]).toEqual({ int: 13 })
    expect(result[199]).toEqual({ int: 13 })
  })

  it('throws for wrong type specified', () => {
    expect(() => parquetWriteBuffer({ columnData: [{ name: 'int', data: [1, 2, 3], type: 'INT64' }] }))
      .toThrow('parquet expected bigint value')
    expect(() => parquetWriteBuffer({ columnData: [{ name: 'int', data: [1n, 2n, 3n], type: 'INT32' }] }))
      .toThrow('parquet expected integer value')
    expect(() => parquetWriteBuffer({ columnData: [{ name: 'int', data: [1, 2, 3n], type: 'INT32' }] }))
      .toThrow('parquet expected integer value')
    expect(() => parquetWriteBuffer({ columnData: [{ name: 'int', data: [1, 2, 3.5], type: 'INT32' }] }))
      .toThrow('parquet expected integer value')
    expect(() => parquetWriteBuffer({ columnData: [{ name: 'int', data: [1n, 2n, 3n], type: 'FLOAT' }] }))
      .toThrow('parquet expected number value')
    expect(() => parquetWriteBuffer({ columnData: [{ name: 'int', data: [1n, 2n, 3n], type: 'DOUBLE' }] }))
      .toThrow('parquet expected number value')
    expect(() => parquetWriteBuffer({ columnData: [{ name: 'int', data: [1, 2, 3], type: 'BYTE_ARRAY' }] }))
      .toThrow('parquet expected Uint8Array value')
    expect(() => parquetWriteBuffer({ columnData: [{ name: 'float16', data: [1n, 2n, 3n], type: 'FLOAT16' }] }))
      .toThrow('parquet float16 expected number value')
    expect(() => parquetWriteBuffer({ columnData: [{ name: 'uuid', data: [new Uint8Array(4)], type: 'UUID' }] }))
      .toThrow('parquet expected Uint8Array of length 16')
  })

  it('throws for empty column with no type specified', () => {
    expect(() => parquetWriteBuffer({ columnData: [{ name: 'empty', data: [] }] }))
      .toThrow('column empty cannot determine type')
    expect(() => parquetWriteBuffer({ columnData: [{ name: 'empty', data: [null, null, null, null] }] }))
      .toThrow('column empty cannot determine type')
  })

  it('throws for mixed types', () => {
    expect(() => parquetWriteBuffer({ columnData: [{ name: 'mixed', data: [1, 2, 3, 'boom'] }] }))
      .toThrow('mixed types not supported')
  })

  it('throws error when columns have mismatched lengths', () => {
    expect(() => parquetWriteBuffer({ columnData: [
      { name: 'col1', data: [1, 2, 3] },
      { name: 'col2', data: [4, 5] },
    ] })).toThrow('columns must have the same length')
  })

  it('throws error for unsupported data types', () => {
    expect(() => parquetWriteBuffer({ columnData: [{ name: 'func', data: [() => {}] }] }))
      .toThrow('cannot determine parquet type for: () => {}')
  })
})
