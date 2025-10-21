import { getSchemaPath } from 'hyparquet/src/schema.js'
import { writeColumn } from './column.js'
import { writeMetadata } from './metadata.js'

/**
 * ParquetWriter class allows incremental writing of parquet files.
 *
 * @import {ColumnChunk, FileMetaData, KeyValue, RowGroup, SchemaElement} from 'hyparquet'
 * @import {ColumnEncoder, ColumnSource, Writer} from '../src/types.js'
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
  this.num_rows = 0n

  // write header PAR1
  this.writer.appendUint32(0x31524150)
}

/**
 * Write data to the file.
 * Will split data into row groups of the specified size.
 *
 * @param {object} options
 * @param {ColumnSource[]} options.columnData
 * @param {number | number[]} [options.rowGroupSize]
 */
ParquetWriter.prototype.write = function({ columnData, rowGroupSize = 100000 }) {
  const columnDataRows = columnData[0]?.data?.length || 0
  for (const { groupStartIndex, groupSize } of groupIterator({ columnDataRows, rowGroupSize })) {
    const groupStartOffset = this.writer.offset

    // row group columns
    /** @type {ColumnChunk[]} */
    const columns = []

    // write columns
    for (let j = 0; j < columnData.length; j++) {
      const { name, data } = columnData[j]
      const groupData = data.slice(groupStartIndex, groupStartIndex + groupSize)

      const schemaTree = getSchemaPath(this.schema, [name])
      // Dive into the leaf element
      while (true) {
        const child = schemaTree[schemaTree.length - 1]
        if (!child.element.num_children) {
          break
        } else if (child.element.num_children === 1) {
          schemaTree.push(child.children[0])
        } else {
          throw new Error(`parquet column ${name} struct unsupported`)
        }
      }
      const schemaPath = schemaTree.map(node => node.element)
      const element = schemaPath.at(-1)
      if (!element) throw new Error(`parquet column ${name} missing schema element`)
      /** @type {ColumnEncoder} */
      const column = {
        columnName: name,
        element,
        schemaPath,
        compressed: this.compressed,
      }

      const file_offset = BigInt(this.writer.offset)
      const meta_data = writeColumn(
        this.writer,
        column,
        groupData,
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

/**
 * Create an iterator for row groups based on the specified row group size.
 * If rowGroupSize is an array, it will return groups based on the sizes in the array.
 * When the array runs out, it will continue with the last size.
 *
 * @param {object} options
 * @param {number} options.columnDataRows - Total number of rows in the column data
 * @param {number | number[]} options.rowGroupSize - Size of each row group or an array of sizes
 * @returns {Array<{groupStartIndex: number, groupSize: number}>}
 */
function groupIterator({ columnDataRows, rowGroupSize }) {
  if (Array.isArray(rowGroupSize) && !rowGroupSize.length) {
    throw new Error('rowGroupSize array cannot be empty')
  }
  const groups = []
  let groupIndex = 0
  let groupStartIndex = 0
  while (groupStartIndex < columnDataRows) {
    const size = Array.isArray(rowGroupSize)
      ? rowGroupSize[Math.min(groupIndex, rowGroupSize.length - 1)]
      : rowGroupSize
    const groupSize = Math.min(size, columnDataRows - groupStartIndex)
    groups.push({ groupStartIndex, groupSize })
    groupStartIndex += size
    groupIndex++
  }
  return groups
}
