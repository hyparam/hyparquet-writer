import { parquetMetadata, parquetReadObjects } from 'hyparquet'
import { readColumnIndex, readOffsetIndex } from 'hyparquet/src/indexes.js'
import { getSchemaPath } from 'hyparquet/src/schema.js'
import { describe, expect, it } from 'vitest'
import { parquetWriteBuffer } from '../src/index.js'

/** @import {ColumnSource} from '../src/types.js' */

describe('parquetWrite columnIndex and offsetIndex', () => {
  it('writes column index and offset index when both are true', async () => {
    const numRows = 100
    /** @type {ColumnSource[]} */
    const columnData = [
      {
        name: 'id',
        data: Array.from({ length: numRows }, (_, i) => i),
        type: 'INT32',
        columnIndex: true,
        offsetIndex: true,
      },
      {
        name: 'value',
        data: Array.from({ length: numRows }, (_, i) => i * 10),
        type: 'INT32',
        columnIndex: true,
        offsetIndex: true,
      },
    ]

    const buffer = parquetWriteBuffer({
      columnData,
      pageSize: 100, // Small pageSize to create multiple pages
    })

    const metadata = parquetMetadata(buffer)

    // Verify index offsets are populated
    const column0 = metadata.row_groups[0].columns[0]
    const column1 = metadata.row_groups[0].columns[1]

    expect(column0.column_index_offset).toBeDefined()
    expect(column0.column_index_length).toBeDefined()
    expect(column0.offset_index_offset).toBeDefined()
    expect(column0.offset_index_length).toBeDefined()

    expect(column1.column_index_offset).toBeDefined()
    expect(column1.column_index_length).toBeDefined()
    expect(column1.offset_index_offset).toBeDefined()
    expect(column1.offset_index_length).toBeDefined()

    // Read back the column index
    const arrayBuffer = buffer.slice(0)
    const columnIndexOffset = Number(column0.column_index_offset)
    const columnIndexLength = Number(column0.column_index_length)
    const columnIndexArrayBuffer = arrayBuffer.slice(columnIndexOffset, columnIndexOffset + columnIndexLength)
    const columnIndexReader = { view: new DataView(columnIndexArrayBuffer), offset: 0 }
    const schemaPath = getSchemaPath(metadata.schema, column0.meta_data?.path_in_schema ?? [])
    const columnIndex = readColumnIndex(columnIndexReader, schemaPath.at(-1)?.element || { name: '' })

    // Verify column index structure
    expect(columnIndex.null_pages).toBeDefined()
    expect(columnIndex.min_values).toBeDefined()
    expect(columnIndex.max_values).toBeDefined()
    expect(columnIndex.boundary_order).toBeDefined()
    expect(columnIndex.null_counts).toBeDefined()

    // pageSize=100 bytes, INT32=4 bytes, 100 values => 5 pages (24+24+24+24+4 values)
    expect(columnIndex.min_values.length).toBe(5)
    expect(columnIndex.max_values.length).toBe(5)
    expect(columnIndex.min_values).toEqual([0, 24, 48, 72, 96])
    expect(columnIndex.max_values).toEqual([23, 47, 71, 95, 99])

    // Read back the offset index
    const offsetIndexOffset = Number(column0.offset_index_offset)
    const offsetIndexLength = Number(column0.offset_index_length)
    const offsetIndexArrayBuffer = arrayBuffer.slice(offsetIndexOffset, offsetIndexOffset + offsetIndexLength)
    const offsetIndexReader = { view: new DataView(offsetIndexArrayBuffer), offset: 0 }
    const offsetIndex = readOffsetIndex(offsetIndexReader)

    // Verify offset index structure
    expect(offsetIndex.page_locations.length).toBe(5)
    const firstRowIndexes = offsetIndex.page_locations.map(pl => pl.first_row_index)
    expect(firstRowIndexes).toEqual([0n, 24n, 48n, 72n, 96n])

    // Data should still be readable
    const rows = await parquetReadObjects({ file: buffer })
    expect(rows.length).toBe(numRows)
    expect(rows[0]).toEqual({ id: 0, value: 0 })
    expect(rows[99]).toEqual({ id: 99, value: 990 })
  })

  it('does not write indexes when neither is enabled', () => {
    const numRows = 100
    /** @type {ColumnSource[]} */
    const columnData = [
      {
        name: 'id',
        data: Array.from({ length: numRows }, (_, i) => i),
        type: 'INT32',
        columnIndex: false,
        offsetIndex: false,
      },
    ]

    const buffer = parquetWriteBuffer({
      columnData,
      pageSize: 100,
    })

    const metadata = parquetMetadata(buffer)
    const column0 = metadata.row_groups[0].columns[0]

    // Indexes should not be present
    expect(column0.column_index_offset).toBeUndefined()
    expect(column0.offset_index_offset).toBeUndefined()
  })

  it('handles per-column opt-out', () => {
    const numRows = 100
    /** @type {ColumnSource[]} */
    const columnData = [
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
    ]

    const buffer = parquetWriteBuffer({
      columnData,
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

  it('handles multiple pages with correct min/max values', () => {
    const numRows = 100
    // Create data that will span multiple pages
    /** @type {ColumnSource[]} */
    const columnData = [
      {
        name: 'id',
        data: Array.from({ length: numRows }, (_, i) => i),
        type: 'INT32',
        columnIndex: true,
      },
    ]

    const buffer = parquetWriteBuffer({
      columnData,
      pageSize: 40, // ~10 INT32 values per page (4 bytes each)
    })

    const metadata = parquetMetadata(buffer)
    const column0 = metadata.row_groups[0].columns[0]
    const arrayBuffer = buffer.slice(0)

    // Read column index
    const columnIndexOffset = Number(column0.column_index_offset)
    const columnIndexLength = Number(column0.column_index_length)
    const columnIndexArrayBuffer = arrayBuffer.slice(columnIndexOffset, columnIndexOffset + columnIndexLength)
    const columnIndexReader = { view: new DataView(columnIndexArrayBuffer), offset: 0 }
    const schemaPath = getSchemaPath(metadata.schema, column0.meta_data?.path_in_schema ?? [])
    const columnIndex = readColumnIndex(columnIndexReader, schemaPath.at(-1)?.element || { name: '' })

    // Should have multiple pages
    expect(columnIndex.min_values.length).toBe(12)
    expect(columnIndex.max_values.length).toBe(12)

    // Values should be ascending (data is 0, 1, 2, ...)
    expect(columnIndex.min_values).toEqual([0, 9, 18, 27, 36, 45, 54, 63, 72, 81, 90, 99])
    expect(columnIndex.max_values).toEqual([8, 17, 26, 35, 44, 53, 62, 71, 80, 89, 98, 99])

    // Read offset index
    const offsetIndexOffset = Number(column0.offset_index_offset)
    const offsetIndexLength = Number(column0.offset_index_length)
    const offsetIndexArrayBuffer = arrayBuffer.slice(offsetIndexOffset, offsetIndexOffset + offsetIndexLength)
    const offsetIndexReader = { view: new DataView(offsetIndexArrayBuffer), offset: 0 }
    const offsetIndex = readOffsetIndex(offsetIndexReader)

    // Should have multiple page locations
    expect(offsetIndex.page_locations.length).toBe(12)

    // First row indexes should be ascending
    const firstRowIndexes = offsetIndex.page_locations.map(pl => pl.first_row_index)
    expect(firstRowIndexes).toEqual([0n, 9n, 18n, 27n, 36n, 45n, 54n, 63n, 72n, 81n, 90n, 99n])
  })

  it('handles nulls in page index', async () => {
    const numRows = 100
    /** @type {ColumnSource[]} */
    const columnData = [
      {
        name: 'nullable',
        data: Array.from({ length: numRows }, (_, i) => i % 5 === 0 ? null : i),
        type: 'INT32',
        nullable: true,
        columnIndex: true,
      },
    ]

    const buffer = parquetWriteBuffer({
      columnData,
      pageSize: 100,
    })

    const metadata = parquetMetadata(buffer)
    const column0 = metadata.row_groups[0].columns[0]
    const arrayBuffer = buffer.slice(0)

    // Read column index
    const columnIndexOffset = Number(column0.column_index_offset)
    const columnIndexLength = Number(column0.column_index_length)
    const columnIndexArrayBuffer = arrayBuffer.slice(columnIndexOffset, columnIndexOffset + columnIndexLength)
    const columnIndexReader = { view: new DataView(columnIndexArrayBuffer), offset: 0 }
    const schemaPath = getSchemaPath(metadata.schema, column0.meta_data?.path_in_schema ?? [])
    const columnIndex = readColumnIndex(columnIndexReader, schemaPath.at(-1)?.element || { name: '' })

    // Should have null counts
    expect(columnIndex.null_counts).toBeDefined()
    expect(columnIndex.null_counts?.some(c => c > 0n)).toBe(true)

    // Data should still be readable
    const rows = await parquetReadObjects({ file: buffer })
    expect(rows.length).toBe(numRows)
    expect(rows[0].nullable).toBe(null) // 0 % 5 === 0
    expect(rows[1].nullable).toBe(1)
  })

  it('writes only column index', () => {
    const numRows = 100
    /** @type {ColumnSource[]} */
    const columnData = [
      {
        name: 'id',
        data: Array.from({ length: numRows }, (_, i) => i),
        type: 'INT32',
        columnIndex: true,
        offsetIndex: false,
      },
    ]

    const buffer = parquetWriteBuffer({
      columnData,
      pageSize: 100,
    })

    const metadata = parquetMetadata(buffer)
    const column0 = metadata.row_groups[0].columns[0]

    // Column index should be present
    expect(column0.column_index_offset).toBeDefined()
    expect(column0.column_index_length).toBeDefined()

    // Offset index should NOT be present
    expect(column0.offset_index_offset).toBeUndefined()
    expect(column0.offset_index_length).toBeUndefined()

    // Read back the column index
    const arrayBuffer = buffer.slice(0)
    const columnIndexOffset = Number(column0.column_index_offset)
    const columnIndexLength = Number(column0.column_index_length)
    const columnIndexArrayBuffer = arrayBuffer.slice(columnIndexOffset, columnIndexOffset + columnIndexLength)
    const columnIndexReader = { view: new DataView(columnIndexArrayBuffer), offset: 0 }
    const schemaPath = getSchemaPath(metadata.schema, column0.meta_data?.path_in_schema ?? [])
    const columnIndex = readColumnIndex(columnIndexReader, schemaPath.at(-1)?.element || { name: '' })

    // Verify column index structure
    expect(columnIndex.min_values).toBeDefined()
    expect(columnIndex.max_values).toBeDefined()
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
    const arrayBuffer = buffer.slice(0)
    const offsetIndexOffset = Number(column0.offset_index_offset)
    const offsetIndexLength = Number(column0.offset_index_length)
    const offsetIndexArrayBuffer = arrayBuffer.slice(offsetIndexOffset, offsetIndexOffset + offsetIndexLength)
    const offsetIndexReader = { view: new DataView(offsetIndexArrayBuffer), offset: 0 }
    const offsetIndex = readOffsetIndex(offsetIndexReader)

    // Verify offset index structure
    expect(offsetIndex.page_locations).toBeDefined()
    expect(offsetIndex.page_locations.length).toBeGreaterThan(0)
  })

  it('does not write indexes for single-page columns', () => {
    // When a column chunk has only one page, columnIndex and offsetIndex provide no value
    // since they are per-page structures meant to help skip pages during predicate pushdown
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
    const data = [1, 10, 2, 11, 3, 12, 4, 13]
    const buffer = parquetWriteBuffer({
      columnData: [{
        name: 'x',
        data,
        type: 'INT32',
        columnIndex: true,
      }],
      pageSize: 9, // 2 INT32 values (8 bytes) per page
    })

    const metadata = parquetMetadata(buffer)
    const column0 = metadata.row_groups[0].columns[0]

    const columnIndexOffset = Number(column0.column_index_offset)
    const columnIndexLength = Number(column0.column_index_length)
    const columnIndexReader = {
      view: new DataView(buffer, columnIndexOffset, columnIndexLength),
      offset: 0,
    }
    const schemaPath = getSchemaPath(metadata.schema, column0.meta_data?.path_in_schema ?? [])
    const columnIndex = readColumnIndex(columnIndexReader, schemaPath.at(-1)?.element || { name: '' })

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
      // Force many small pages so the first list row spans multiple pages.
      pageSize: 16,
    })

    const metadata = parquetMetadata(buffer)
    const column0 = metadata.row_groups[0].columns[0]

    const offsetIndexOffset = Number(column0.offset_index_offset)
    const offsetIndexLength = Number(column0.offset_index_length)
    const offsetIndex = readOffsetIndex({
      view: new DataView(buffer, offsetIndexOffset, offsetIndexLength),
      offset: 0,
    })

    // All pages start inside row 0, so first_row_index should stay 0.
    const firstRowIndexes = offsetIndex.page_locations.map(pl => pl.first_row_index)
    expect(firstRowIndexes).toEqual([0n, 0n, 0n, 0n])
  })
})
