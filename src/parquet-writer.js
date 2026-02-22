import { getSchemaPath } from 'hyparquet/src/schema.js'
import { writeColumn } from './column.js'
import { encodeNestedValues } from './dremel.js'
import { writeIndexes } from './indexes.js'
import { writeMetadata } from './metadata.js'
import { snappyCompress } from './snappy.js'

/**
 * @import {ColumnChunk, CompressionCodec, FileMetaData, KeyValue, RowGroup, SchemaElement, SchemaTree} from 'hyparquet'
 * @import {ColumnEncoder, ColumnSource, Compressors, PageIndexes, Writer} from '../src/types.js'
 */

/**
 * ParquetWriter class allows incremental writing of parquet files.
 *
 * @param {object} options
 * @param {Writer} options.writer
 * @param {SchemaElement[]} options.schema
 * @param {CompressionCodec} [options.codec]
 * @param {Compressors} [options.compressors]
 * @param {boolean} [options.statistics]
 * @param {KeyValue[]} [options.kvMetadata]
 */
export function ParquetWriter({ writer, schema, codec = 'SNAPPY', compressors, statistics = true, kvMetadata }) {
  this.writer = writer
  this.schema = schema
  this.codec = codec
  // Include built-in snappy as fallback
  this.compressors = { SNAPPY: snappyCompress, ...compressors }
  this.statistics = statistics
  this.kvMetadata = kvMetadata

  /** @type {RowGroup[]} */
  this.row_groups = []
  this.num_rows = 0n

  /** @type {PageIndexes[]} */
  this.pendingIndexes = []

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
 * @param {number} [options.pageSize]
 */
ParquetWriter.prototype.write = function({ columnData, rowGroupSize = [1000, 100000], pageSize = 1048576 }) {
  const columnDataRows = columnData[0]?.data?.length || 0
  for (const { groupStartIndex, groupSize } of groupIterator({ columnDataRows, rowGroupSize })) {
    const groupStartOffset = this.writer.offset
    /** @type {ColumnChunk[]} */
    const columns = []

    // write columns
    for (let j = 0; j < columnData.length; j++) {
      const { name, data, encoding, columnIndex = false, offsetIndex = true } = columnData[j]

      // Spec: if ColumnIndex is present, OffsetIndex must also be present
      if (columnIndex && !offsetIndex) {
        throw new Error('parquet ColumnIndex cannot be present without OffsetIndex')
      }
      if (data.length !== columnDataRows) {
        throw new Error('parquet columns must have the same length')
      }

      const groupData = data.slice(groupStartIndex, groupStartIndex + groupSize)
      const columnPath = getSchemaPath(this.schema, [name])
      const leafPaths = getLeafSchemaPaths(columnPath)

      for (const leafPath of leafPaths) {
        const schemaPath = leafPath.map(node => node.element)

        /** @type {ColumnEncoder} */
        const column = {
          columnName: schemaPath.slice(1).map(s => s.name).join('.'),
          element: schemaPath[schemaPath.length - 1],
          schemaPath,
          codec: this.codec,
          compressors: this.compressors,
          stats: this.statistics,
          pageSize,
          columnIndex,
          offsetIndex,
          encoding,
        }

        const pageData = encodeNestedValues(leafPath, groupData)
        const result = writeColumn({
          writer: this.writer,
          column,
          pageData,
        })

        columns.push(result.chunk)
        this.pendingIndexes.push(result)
      }
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
  // Write all indexes at end of file
  writeIndexes(this.writer, this.pendingIndexes)

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

/**
 * Expand a schema path to all primitive leaf nodes under the column.
 *
 * @param {SchemaTree[]} schemaPath
 * @returns {SchemaTree[][]}
 */
function getLeafSchemaPaths(schemaPath) {
  /** @type {SchemaTree[][]} */
  const leaves = []
  dfs(schemaPath)
  return leaves

  /**
   * @param {SchemaTree[]} path
   */
  function dfs(path) {
    const node = path[path.length - 1]
    if (!node.children.length) {
      leaves.push(path)
      return
    }
    for (const child of node.children) {
      dfs([...path, child])
    }
  }
}
