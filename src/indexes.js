import { BoundaryOrders } from 'hyparquet/src/constants.js'
import { serializeTCompactProtocol } from './thrift.js'

/**
 * Write ColumnIndex and OffsetIndex to the writer.
 *
 * @import {ColumnChunk} from 'hyparquet'
 * @import {PageIndexes, Writer} from '../src/types.js'
 * @param {Writer} writer
 * @param {ColumnChunk} columnChunk
 * @param {PageIndexes} pageIndexes
 */
export function writeIndexes(writer, columnChunk, { columnIndex, offsetIndex }) {
  // Write ColumnIndex
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

  // Write OffsetIndex
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
