import { BoundaryOrders } from 'hyparquet/src/constants.js'
import { serializeTCompactProtocol } from './thrift.js'

/**
 * @import {ColumnChunk, ColumnIndex, OffsetIndex} from 'hyparquet'
 * @import {PageIndexes, Writer} from '../src/types.js'
 */

/**
 * Write ColumnIndex and OffsetIndex for the given columns.
 *
 * @param {Writer} writer
 * @param {PageIndexes[]} pageIndexes
 */
export function writeIndexes(writer, pageIndexes) {
  for (const { chunk, columnIndex } of pageIndexes) {
    writeColumnIndex(writer, chunk, columnIndex)
  }
  for (const { chunk, offsetIndex } of pageIndexes) {
    writeOffsetIndex(writer, chunk, offsetIndex)
  }
}

/**
 * @param {Writer} writer
 * @param {ColumnChunk} columnChunk
 * @param {ColumnIndex} [columnIndex]
 */
function writeColumnIndex(writer, columnChunk, columnIndex) {
  // Page indexes only help when multiple pages
  if (!columnIndex || columnIndex.min_values.length <= 1) return
  const columnIndexOffset = writer.offset
  serializeTCompactProtocol(writer, {
    field_1: columnIndex.null_pages,
    field_2: columnIndex.min_values,
    field_3: columnIndex.max_values,
    field_4: BoundaryOrders.indexOf(columnIndex.boundary_order),
    field_5: columnIndex.null_counts,
  })
  columnChunk.column_index_offset = BigInt(columnIndexOffset)
  columnChunk.column_index_length = writer.offset - columnIndexOffset
}

/**
 * @param {Writer} writer
 * @param {ColumnChunk} columnChunk
 * @param {OffsetIndex} [offsetIndex]
 */
function writeOffsetIndex(writer, columnChunk, offsetIndex) {
  // Page indexes only help when multiple pages
  if (!offsetIndex || offsetIndex.page_locations.length <= 1) return
  const offsetIndexOffset = writer.offset
  serializeTCompactProtocol(writer, {
    field_1: offsetIndex.page_locations.map(p => ({
      field_1: p.offset,
      field_2: p.compressed_page_size,
      field_3: p.first_row_index,
    })),
  })
  columnChunk.offset_index_offset = BigInt(offsetIndexOffset)
  columnChunk.offset_index_length = writer.offset - offsetIndexOffset
}
