import { writeColumn } from './column.js'
import { writeMetadata } from './metadata.js'

/**
 * @typedef {import('hyparquet').ColumnChunk} ColumnChunk
 * @typedef {import('hyparquet').FileMetaData} FileMetaData
 * @typedef {import('hyparquet').KeyValue} KeyValue
 * @typedef {import('hyparquet').RowGroup} RowGroup
 * @typedef {import('hyparquet').SchemaElement} SchemaElement
 * @typedef {import('./types.js').ColumnData} ColumnData
 * @typedef {import('./types.js').Writer} Writer
 */

/**
 * @typedef {object} ParquetWriterOptions
 * @property {Writer} writer - The writer to use
 * @property {SchemaElement[]} schema - The schema
 * @property {boolean} [compressed=true] - Whether to compress data
 * @property {boolean} [statistics=true] - Whether to include statistics
 * @property {KeyValue[]} [kvMetadata] - Key-value metadata
 */

/**
 * @typedef {object} ParquetWriteOptions
 * @property {ColumnData[]} columnData - The columns to write
 * @property {number} [rowGroupSize=100000] - The number of rows per row group
 */

/**
 * ParquetWriter class allows incremental writing of parquet files.
 *
 * @class
 * @param {ParquetWriterOptions} options - Writer options
 */
export function ParquetWriter(options) {
  this.writer = options.writer
  this.schema = options.schema
  this.compressed = options.compressed !== false
  this.statistics = options.statistics !== false
  this.kvMetadata = options.kvMetadata

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
 * @param {ParquetWriteOptions} options - Write options
 */
ParquetWriter.prototype.write = function (options) {
  const columnData = options.columnData
  const rowGroupSize = options.rowGroupSize || 100000

  const columnDataRows = columnData[0]?.data?.length || 0
  for (
    let groupStartIndex = 0;
    groupStartIndex < columnDataRows;
    groupStartIndex += rowGroupSize
  ) {
    const groupStartOffset = this.writer.offset
    const groupSize = Math.min(rowGroupSize, columnDataRows - groupStartIndex)

    // row group columns
    /** @type {ColumnChunk[]} */
    const columns = []

    // write columns
    for (let j = 0; j < columnData.length; j++) {
      const { data } = columnData[j]
      const schemaPath = [this.schema[0], this.schema[j + 1]]
      const groupData = data.slice(
        groupStartIndex,
        groupStartIndex + groupSize
      )
      const file_offset = BigInt(this.writer.offset)
      const meta_data = writeColumn(
        this.writer,
        schemaPath,
        groupData,
        this.compressed,
        this.statistics
      )

      // save column chunk metadata
      columns.push({
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
ParquetWriter.prototype.finish = function () {
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
