import { parquetMetadata, parquetReadObjects } from 'hyparquet'
import { describe, expect, it } from 'vitest'
import { parquetWriteBuffer } from '../src/index.js'

/**
 * @import {ColumnSource} from '../src/types.js'
 */

describe('parquetWrite multi-page', () => {
  it('writes with small pageSize and data is still readable', async () => {
    // Generate enough data to span multiple pages with a small pageSize
    const numRows = 1000
    /** @type {ColumnSource[]} */
    const columnData = [
      { name: 'id', data: Array.from({ length: numRows }, (_, i) => i), type: 'INT32' },
      { name: 'value', data: Array.from({ length: numRows }, (_, i) => i * 2), type: 'INT32' },
    ]

    // Use a very small page size to force multiple pages
    // Each INT32 is 4 bytes, so 100 bytes should hold about 25 values per page
    const buffer = parquetWriteBuffer({
      columnData,
      pageSize: 100,
    })

    // Read back the data
    const rows = await parquetReadObjects({ file: buffer })

    expect(rows.length).toBe(numRows)
    expect(rows[0]).toEqual({ id: 0, value: 0 })
    expect(rows[999]).toEqual({ id: 999, value: 1998 })
  })

  it('handles various data types with pageSize', async () => {
    const numRows = 500
    /** @type {ColumnSource[]} */
    const columnData = [
      { name: 'int32', data: Array.from({ length: numRows }, (_, i) => i), type: 'INT32' },
      { name: 'int64', data: Array.from({ length: numRows }, (_, i) => BigInt(i)), type: 'INT64' },
      { name: 'float', data: Array.from({ length: numRows }, (_, i) => i * 0.5), type: 'FLOAT' },
      { name: 'double', data: Array.from({ length: numRows }, (_, i) => i * 0.5), type: 'DOUBLE' },
      { name: 'bool', data: Array.from({ length: numRows }, (_, i) => i % 2 === 0), type: 'BOOLEAN' },
    ]

    const buffer = parquetWriteBuffer({
      columnData,
      pageSize: 200,
      statistics: true,
    })

    const rows = await parquetReadObjects({ file: buffer })

    expect(rows.length).toBe(numRows)
    expect(rows[0].int32).toBe(0)
    expect(rows[0].bool).toBe(true)
    expect(rows[1].bool).toBe(false)
  })

  it('handles strings with pageSize', async () => {
    const numRows = 100
    const strings = Array.from({ length: numRows }, (_, i) => `string_value_${i}`)
    /** @type {ColumnSource[]} */
    const columnData = [
      { name: 'id', data: Array.from({ length: numRows }, (_, i) => i), type: 'INT32' },
      { name: 'str', data: strings, type: 'STRING' },
    ]

    const buffer = parquetWriteBuffer({
      columnData,
      pageSize: 200,
    })

    const rows = await parquetReadObjects({ file: buffer })

    expect(rows.length).toBe(numRows)
    expect(rows[0].str).toBe('string_value_0')
    expect(rows[99].str).toBe('string_value_99')
  })

  it('handles nulls with pageSize', async () => {
    const numRows = 200
    /** @type {ColumnSource[]} */
    const columnData = [
      { name: 'id', data: Array.from({ length: numRows }, (_, i) => i), type: 'INT32' },
      { name: 'nullable', data: Array.from({ length: numRows }, (_, i) => i % 3 === 0 ? null : i), type: 'INT32', nullable: true },
    ]

    const buffer = parquetWriteBuffer({
      columnData,
      pageSize: 100,
    })

    const rows = await parquetReadObjects({ file: buffer })

    expect(rows.length).toBe(numRows)
    expect(rows[0].nullable).toBe(null)
    expect(rows[1].nullable).toBe(1)
    expect(rows[2].nullable).toBe(2)
    expect(rows[3].nullable).toBe(null)
  })

  it('works without pageSize (backwards compatibility)', async () => {
    const numRows = 100
    /** @type {ColumnSource[]} */
    const columnData = [
      { name: 'id', data: Array.from({ length: numRows }, (_, i) => i), type: 'INT32' },
    ]

    // No pageSize specified
    const buffer = parquetWriteBuffer({ columnData })

    const rows = await parquetReadObjects({ file: buffer })
    expect(rows.length).toBe(numRows)
  })

  it('handles single value per page edge case', async () => {
    const numRows = 10
    /** @type {ColumnSource[]} */
    const columnData = [
      { name: 'id', data: Array.from({ length: numRows }, (_, i) => i), type: 'INT32' },
    ]

    // Very tiny pageSize - should still work
    const buffer = parquetWriteBuffer({
      columnData,
      pageSize: 4, // exactly one INT32
    })

    const rows = await parquetReadObjects({ file: buffer })
    expect(rows.length).toBe(numRows)
    expect(rows.map(r => r.id)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
  })

  it('handles dictionary encoding with pageSize', async () => {
    // Use repeated values to trigger dictionary encoding
    const numRows = 500
    const values = ['apple', 'banana', 'cherry']
    /** @type {ColumnSource[]} */
    const columnData = [
      { name: 'id', data: Array.from({ length: numRows }, (_, i) => i), type: 'INT32' },
      { name: 'fruit', data: Array.from({ length: numRows }, (_, i) => values[i % 3]), type: 'STRING' },
    ]

    const buffer = parquetWriteBuffer({
      columnData,
      pageSize: 100,
    })

    // should use dictionary encoding
    const metadata = parquetMetadata(buffer)
    const column1 = metadata.row_groups[0].columns[1]
    expect(column1.meta_data?.encodings).toContain('RLE_DICTIONARY')

    const rows = await parquetReadObjects({ file: buffer })
    expect(rows.length).toBe(numRows)
    expect(rows[0].fruit).toBe('apple')
    expect(rows[1].fruit).toBe('banana')
    expect(rows[2].fruit).toBe('cherry')
    expect(rows[3].fruit).toBe('apple')
  })
})
