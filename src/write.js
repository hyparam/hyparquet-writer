import { writeColumn } from './column.js'
import { Writer } from './writer.js'
import { writeMetadata } from './metadata.js'
import { getSchemaElementForValues } from './schema.js'

/**
 * Write data as parquet to an ArrayBuffer
 *
 * @import {ColumnChunk, DecodedArray, FileMetaData, RowGroup, SchemaElement, SchemaTree} from 'hyparquet'
 * @import {KeyValue} from 'hyparquet/src/types.js'
 * @import {ColumnData} from '../src/types.js'
 * @param {object} options
 * @param {ColumnData[]} options.columnData
 * @param {boolean} [options.compressed]
 * @param {boolean} [options.statistics]
 * @param {number} [options.rowGroupSize]
 * @param {KeyValue[]} [options.kvMetadata]
 * @returns {ArrayBuffer}
 */
export function parquetWrite({ columnData, compressed = true, statistics = true, rowGroupSize = 100000, kvMetadata }) {
  const num_rows = columnData.length ? BigInt(columnData[0].data.length) : 0n
  const writer = new Writer()

  // construct schema
  /** @type {SchemaElement[]} */
  const schema = [{
    name: 'root',
    num_children: columnData.length,
  }]
  for (const { name, data, type } of columnData) {
    // check if all columns have the same length
    if (BigInt(data.length) !== num_rows) {
      throw new Error('columns must have the same length')
    }
    // auto-detect type
    const schemaElement = getSchemaElementForValues(name, data, type)
    if (!schemaElement.type) throw new Error(`column ${name} cannot determine type`)
    schema.push(schemaElement)
  }

  // write header PAR1
  writer.appendUint32(0x31524150)

  /** @type {RowGroup[]} */
  const row_groups = []
  for (let i = 0; i < num_rows; i += rowGroupSize) {
    const groupStart = writer.offset

    // row group columns
    /** @type {ColumnChunk[]} */
    const columns = []

    // write columns
    for (let i = 0; i < columnData.length; i++) {
      const { name, data } = columnData[i]
      const file_offset = BigInt(writer.offset)
      const schemaPath = [schema[0], schema[i + 1]]
      const meta_data = writeColumn(writer, schemaPath, data, compressed, statistics)

      // save metadata
      columns.push({
        file_path: name,
        file_offset,
        meta_data,
      })
    }

    row_groups.push({
      columns,
      total_byte_size: BigInt(writer.offset - groupStart),
      num_rows: BigInt(Math.min(rowGroupSize, Number(num_rows) - i)),
    })
  }

  // write metadata
  /** @type {FileMetaData} */
  const metadata = {
    version: 2,
    created_by: 'hyparquet',
    schema,
    num_rows,
    row_groups,
    metadata_length: 0,
    key_value_metadata: kvMetadata,
  }
  // @ts-ignore don't want to actually serialize metadata_length
  delete metadata.metadata_length
  writeMetadata(writer, metadata)

  // write footer PAR1
  writer.appendUint32(0x31524150)

  return writer.getBuffer()
}
