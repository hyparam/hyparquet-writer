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
      { name: 'id', data: Array.from({ length: numRows }, (_, i) => i), type: 'INT32', columnIndex: true, offsetIndex: true },
      { name: 'value', data: Array.from({ length: numRows }, (_, i) => i * 10), type: 'INT32', columnIndex: true, offsetIndex: true },
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
      { name: 'id', data: Array.from({ length: numRows }, (_, i) => i), type: 'INT32' },
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

  it('handles per-column opt-in (first column both indexes)', () => {
    const numRows = 100
    /** @type {ColumnSource[]} */
    const columnData = [
      { name: 'indexed', data: Array.from({ length: numRows }, (_, i) => i), type: 'INT32', columnIndex: true, offsetIndex: true },
      { name: 'not_indexed', data: Array.from({ length: numRows }, (_, i) => i * 2), type: 'INT32' },
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

  it('handles per-column opt-in (second column both indexes)', async () => {
    const numRows = 100
    /** @type {ColumnSource[]} */
    const columnData = [
      { name: 'id', data: Array.from({ length: numRows }, (_, i) => i), type: 'INT32' },
      { name: 'text', data: Array.from({ length: numRows }, (_, i) => `text${i}`), type: 'STRING', columnIndex: true, offsetIndex: true },
    ]

    const buffer = parquetWriteBuffer({
      columnData,
      pageSize: 100,
    })

    const metadata = parquetMetadata(buffer)
    const idColumn = metadata.row_groups[0].columns[0]
    const textColumn = metadata.row_groups[0].columns[1]

    // id column should NOT have indexes (no pageIndex requested)
    expect(idColumn.column_index_offset).toBeUndefined()
    expect(idColumn.offset_index_offset).toBeUndefined()

    // text column should have indexes
    expect(textColumn.column_index_offset).toBeDefined()
    expect(textColumn.offset_index_offset).toBeDefined()

    // Read back the column index for text column
    const arrayBuffer = buffer.slice(0)
    const columnIndexOffset = Number(textColumn.column_index_offset)
    const columnIndexLength = Number(textColumn.column_index_length)
    const columnIndexArrayBuffer = arrayBuffer.slice(columnIndexOffset, columnIndexOffset + columnIndexLength)
    const columnIndexReader = { view: new DataView(columnIndexArrayBuffer), offset: 0 }
    const schemaPath = getSchemaPath(metadata.schema, textColumn.meta_data?.path_in_schema ?? [])
    const columnIndex = readColumnIndex(columnIndexReader, schemaPath.at(-1)?.element || { name: '' })

    // Verify column index contains text data (strings), not integer data
    // The min/max values should be strings like 'text0', 'text1', etc.
    expect(columnIndex.min_values).toBeDefined()
    expect(columnIndex.max_values).toBeDefined()
    expect(typeof columnIndex.min_values[0]).toBe('string')
    expect(columnIndex.min_values[0]).toMatch(/^text/)

    // Data should still be readable and correct
    const rows = await parquetReadObjects({ file: buffer })
    expect(rows.length).toBe(numRows)
    expect(rows[0]).toEqual({ id: 0, text: 'text0' })
    expect(rows[99]).toEqual({ id: 99, text: 'text99' })
  })

  it('handles multiple pages with correct min/max values', () => {
    const numRows = 100
    // Create data that will span multiple pages
    /** @type {ColumnSource[]} */
    const columnData = [
      { name: 'id', data: Array.from({ length: numRows }, (_, i) => i), type: 'INT32', columnIndex: true, offsetIndex: true },
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
        offsetIndex: true,
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

  it('writes only column index when offsetIndex is false', () => {
    const numRows = 100
    /** @type {ColumnSource[]} */
    const columnData = [
      { name: 'id', data: Array.from({ length: numRows }, (_, i) => i), type: 'INT32', columnIndex: true },
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

  it('writes only offset index when columnIndex is false', () => {
    const numRows = 100
    /** @type {ColumnSource[]} */
    const columnData = [
      { name: 'id', data: Array.from({ length: numRows }, (_, i) => i), type: 'INT32', offsetIndex: true },
    ]

    const buffer = parquetWriteBuffer({
      columnData,
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

  it('handles mixed index options per column', () => {
    const numRows = 100
    /** @type {ColumnSource[]} */
    const columnData = [
      { name: 'col_index_only', data: Array.from({ length: numRows }, (_, i) => i), type: 'INT32', columnIndex: true },
      { name: 'offset_index_only', data: Array.from({ length: numRows }, (_, i) => i * 2), type: 'INT32', offsetIndex: true },
      { name: 'both_indexes', data: Array.from({ length: numRows }, (_, i) => i * 3), type: 'INT32', columnIndex: true, offsetIndex: true },
      { name: 'no_indexes', data: Array.from({ length: numRows }, (_, i) => i * 4), type: 'INT32' },
    ]

    const buffer = parquetWriteBuffer({
      columnData,
      pageSize: 100,
    })

    const metadata = parquetMetadata(buffer)
    const cols = metadata.row_groups[0].columns

    // col_index_only: only column index
    expect(cols[0].column_index_offset).toBeDefined()
    expect(cols[0].offset_index_offset).toBeUndefined()

    // offset_index_only: only offset index
    expect(cols[1].column_index_offset).toBeUndefined()
    expect(cols[1].offset_index_offset).toBeDefined()

    // both_indexes: both
    expect(cols[2].column_index_offset).toBeDefined()
    expect(cols[2].offset_index_offset).toBeDefined()

    // no_indexes: neither
    expect(cols[3].column_index_offset).toBeUndefined()
    expect(cols[3].offset_index_offset).toBeUndefined()
  })
})
