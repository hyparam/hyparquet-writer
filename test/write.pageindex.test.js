import { parquetMetadata, parquetReadObjects } from 'hyparquet'
import { readColumnIndex, readOffsetIndex } from 'hyparquet/src/indexes.js'
import { describe, expect, it } from 'vitest'
import { parquetWriteBuffer } from '../src/index.js'

/**
 * @import {DataReader} from 'hyparquet'
 */

const numRows = 100

/**
 * @param {ArrayBuffer} buffer
 * @param {bigint} [offset]
 * @param {number} [length]
 * @returns {DataReader}
 */
function indexReader(buffer, offset, length) {
  return { view: new DataView(buffer, Number(offset), length), offset: 0 }
}

describe('parquetWrite columnIndex and offsetIndex', () => {
  it('writes column index and offset index when both are true', async () => {
    const buffer = parquetWriteBuffer({
      columnData: [
        {
          name: 'value',
          data: Array.from({ length: numRows }, (_, i) => (i - 50) ** 2), // non-monotonic
          type: 'INT32',
          columnIndex: true,
          offsetIndex: true,
        },
      ],
      pageSize: 100, // Small pageSize to create multiple pages
    })
    const metadata = parquetMetadata(buffer)
    const column0 = metadata.row_groups[0].columns[0]

    // Verify index offsets are populated
    expect(column0.column_index_offset).toBeDefined()
    expect(column0.column_index_length).toBeDefined()
    expect(column0.offset_index_offset).toBeDefined()
    expect(column0.offset_index_length).toBeDefined()

    // Read back the column index
    const columnIndexReader = indexReader(buffer, column0.column_index_offset, column0.column_index_length)
    const columnIndex = readColumnIndex(columnIndexReader, metadata.schema[1])
    expect(columnIndex.null_pages).toEqual([false, false, false, false, false])
    expect(columnIndex.min_values).toEqual([729, 9, 0, 484, 2116])
    expect(columnIndex.max_values).toEqual([2500, 676, 441, 2025, 2401])
    expect(columnIndex.boundary_order).toBe('UNORDERED')
    expect(columnIndex.null_counts).toEqual([0n, 0n, 0n, 0n, 0n])

    // Read back the offset index
    const offsetIndexReader = indexReader(buffer, column0.offset_index_offset, column0.offset_index_length)
    const offsetIndex = readOffsetIndex(offsetIndexReader)
    // First row indexes should be ascending
    const firstRowIndexes = offsetIndex.page_locations.map(pl => pl.first_row_index)
    expect(firstRowIndexes).toEqual([0n, 24n, 48n, 72n, 96n])

    // Data should still be readable
    const rows = await parquetReadObjects({ file: buffer })
    expect(rows.length).toBe(numRows)
    expect(rows[0]).toEqual({ value: 2500 })
    expect(rows[50]).toEqual({ value: 0 })
    expect(rows[99]).toEqual({ value: 2401 })
  })

  it('handles per-column opt-out', () => {
    const buffer = parquetWriteBuffer({
      columnData: [
        {
          name: 'indexed',
          data: Array.from({ length: numRows }, (_, i) => i),
          type: 'INT32',
          columnIndex: true,
        },
        {
          name: 'not_indexed',
          data: Array.from({ length: numRows }, (_, i) => i * 2),
          type: 'INT32',
          offsetIndex: false,
        },
      ],
      pageSize: 100,
    })
    const metadata = parquetMetadata(buffer)
    const indexedColumn = metadata.row_groups[0].columns[0]
    const notIndexedColumn = metadata.row_groups[0].columns[1]

    // Indexed column should have indexes
    expect(indexedColumn.column_index_offset).toBeDefined()
    expect(indexedColumn.offset_index_offset).toBeDefined()

    // Non-indexed column should not
    expect(notIndexedColumn.column_index_offset).toBeUndefined()
    expect(notIndexedColumn.offset_index_offset).toBeUndefined()
  })

  it('handles nulls in page index', async () => {
    const buffer = parquetWriteBuffer({
      columnData: [
        {
          name: 'nullable',
          data: Array.from({ length: numRows }, (_, i) => i % 5 === 0 ? null : i),
          type: 'INT32',
          nullable: true,
          columnIndex: true,
        },
      ],
      pageSize: 100,
    })
    const metadata = parquetMetadata(buffer)
    const column0 = metadata.row_groups[0].columns[0]

    // Read column index
    const columnIndexReader = indexReader(buffer, column0.column_index_offset, column0.column_index_length)
    const columnIndex = readColumnIndex(columnIndexReader, metadata.schema[1])

    // Should have null counts
    expect(columnIndex.null_pages).toEqual([false, false, false, false])
    expect(columnIndex.min_values).toEqual([1, 31, 61, 91])
    expect(columnIndex.max_values).toEqual([29, 59, 89, 99])
    expect(columnIndex.boundary_order).toBe('ASCENDING')
    expect(columnIndex.null_counts).toEqual([7n, 6n, 6n, 1n])

    // Data should still be readable
    const rows = await parquetReadObjects({ file: buffer })
    expect(rows.length).toBe(numRows)
    expect(rows[0].nullable).toBe(null) // 0 % 5 === 0
    expect(rows[1].nullable).toBe(1)
  })

  it('writes only offset index by default', () => {
    const data = Array.from({ length: 100 }, (_, i) => i)
    const buffer = parquetWriteBuffer({
      columnData: [
        { name: 'id', data, type: 'INT32' },
      ],
      pageSize: 100,
    })
    const metadata = parquetMetadata(buffer)
    const column0 = metadata.row_groups[0].columns[0]

    // Column index should NOT be present
    expect(column0.column_index_offset).toBeUndefined()
    expect(column0.column_index_length).toBeUndefined()

    // Offset index should be present
    expect(column0.offset_index_offset).toBeDefined()
    expect(column0.offset_index_length).toBeDefined()

    // Read back the offset index
    const offsetIndexReader = indexReader(buffer, column0.offset_index_offset, column0.offset_index_length)
    const offsetIndex = readOffsetIndex(offsetIndexReader)
    expect(offsetIndex.page_locations.length).toBe(5)
  })

  it('does not write indexes for single-page columns', () => {
    const data = Array.from({ length: 10 }, (_, i) => i)
    const buffer = parquetWriteBuffer({ columnData: [
      { name: 'id', data, type: 'INT32', columnIndex: true },
    ] })
    const metadata = parquetMetadata(buffer)
    const column0 = metadata.row_groups[0].columns[0]

    // Even though columnIndex and offsetIndex were requested, they should not be written
    // for a single-page column since they provide no benefit
    expect(column0.column_index_offset).toBeUndefined()
    expect(column0.column_index_length).toBeUndefined()
    expect(column0.offset_index_offset).toBeUndefined()
    expect(column0.offset_index_length).toBeUndefined()
  })

  it('correctly reports ASCENDING boundary order for overlapping page ranges', () => {
    // Data where each page has overlapping ranges but min/max are ascending
    // Pages: [1,10], [2,11], [3,12], [4,13]
    // min_values: [1, 2, 3, 4] ascending
    // max_values: [10, 11, 12, 13] ascending
    const buffer = parquetWriteBuffer({
      columnData: [{
        name: 'x',
        data: [1, 10, 2, 11, 3, 12, 4, 13],
        type: 'INT32',
        columnIndex: true,
      }],
      pageSize: 9, // 2 values per page
    })
    const metadata = parquetMetadata(buffer)
    const column0 = metadata.row_groups[0].columns[0]

    // Read back the column index
    const columnIndexReader = indexReader(buffer, column0.column_index_offset, column0.column_index_length)
    const columnIndex = readColumnIndex(columnIndexReader, metadata.schema[1])
    expect(columnIndex.min_values).toEqual([1, 2, 3, 4])
    expect(columnIndex.max_values).toEqual([10, 11, 12, 13])
    expect(columnIndex.boundary_order).toBe('ASCENDING')
  })

  it('keeps first_row_index stable when a repeated row spans multiple pages', () => {
    const buffer = parquetWriteBuffer({
      columnData: [{
        name: 'vals',
        data: [
          Array.from({ length: 10 }, (_, i) => i + 1),
          [11],
          [12],
        ],
      }],
      schema: [
        { name: 'root', num_children: 1 },
        { name: 'vals', repetition_type: 'OPTIONAL', num_children: 1, converted_type: 'LIST' },
        { name: 'list', repetition_type: 'REPEATED', num_children: 1 },
        { name: 'element', repetition_type: 'OPTIONAL', type: 'INT32' },
      ],
      pageSize: 16, // first list spans multiple pages
    })
    const metadata = parquetMetadata(buffer)
    const column0 = metadata.row_groups[0].columns[0]

    const offsetIndexReader = indexReader(buffer, column0.offset_index_offset, column0.offset_index_length)
    const offsetIndex = readOffsetIndex(offsetIndexReader)

    // All pages start inside row 0, so first_row_index should stay 0.
    const firstRowIndexes = offsetIndex.page_locations.map(pl => pl.first_row_index)
    expect(firstRowIndexes).toEqual([0n, 0n, 0n, 0n])
  })
})
