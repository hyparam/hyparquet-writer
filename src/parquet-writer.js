import { writeColumn } from './column.js'
import { writeMetadata } from './metadata.js'

/**
 * Create a new ParquetWriter.
 *
 * @import {ColumnChunk, FileMetaData, RowGroup, SchemaElement} from 'hyparquet'
 * @import {KeyValue} from 'hyparquet/src/types.js'
 * @import {ColumnData, Writer} from '../src/types.js'
 * @param {object} options
 * @param {Writer} options.writer
 * @param {SchemaElement[]} options.schema
 * @param {boolean} [options.compressed]
 * @param {boolean} [options.statistics]
 * @param {KeyValue[]} [options.kvMetadata]
 */
export function ParquetWriter({ writer, schema, compressed = true, statistics = true, kvMetadata }) {
  this.writer = writer
  this.schema = schema
  this.compressed = compressed
  this.statistics = statistics
  this.kvMetadata = kvMetadata

  /** @type {RowGroup[]} */
  this.row_groups = []
  this.num_rows = BigInt(0)

  // write header PAR1
  this.writer.appendUint32(0x31524150)
}

/**
 * Write data to the file.
 * Will split data into row groups of the specified size.
 *
 * @param {object} options
 * @param {ColumnData[]} options.columnData
 * @param {number} [options.rowGroupSize]
 */
ParquetWriter.prototype.write = function({ columnData, rowGroupSize = 100000 }) {
  const columnDataRows = columnData[0]?.data?.length || 0
  for (let groupStartIndex = 0; groupStartIndex < columnDataRows; groupStartIndex += rowGroupSize) {
    const groupStartOffset = this.writer.offset
    const groupSize = Math.min(rowGroupSize, columnDataRows - groupStartIndex)

    // row group columns
    /** @type {ColumnChunk[]} */
    const columns = []

    // write columns
    for (let j = 0; j < columnData.length; j++) {
      const { name, data } = columnData[j]
      const schemaPath = [this.schema[0], this.schema[j + 1]]
      const groupData = data.slice(groupStartIndex, groupStartIndex + groupSize)
      const file_offset = BigInt(this.writer.offset)
      const meta_data = writeColumn(this.writer, schemaPath, groupData, this.compressed, this.statistics)

      // save column chunk metadata
      columns.push({
        file_path: name,
        file_offset,
        meta_data,
      })
    }
    this.num_rows += BigInt(groupSize)

    this.row_groups.push({
      columns,
      total_byte_size: BigInt(this.writer.offset - groupStartOffset),
      num_rows: BigInt(groupSize),
    })
  }
}

/**
 * Finish writing the file.
 */
ParquetWriter.prototype.finish = function() {
  // write metadata
  /** @type {FileMetaData} */
  const metadata = {
    version: 2,
    created_by: 'hyparquet',
    schema: this.schema,
    num_rows: this.num_rows,
    row_groups: this.row_groups,
    metadata_length: 0,
    key_value_metadata: this.kvMetadata,
  }
  // @ts-ignore don't want to actually serialize metadata_length
  delete metadata.metadata_length
  writeMetadata(this.writer, metadata)

  // write footer PAR1
  this.writer.appendUint32(0x31524150)
  this.writer.finish()
}
