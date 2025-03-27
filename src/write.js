import { writeColumn } from './column.js'
import { Writer } from './writer.js'
import { writeMetadata } from './metadata.js'
import { getSchemaElementForValues } from './schema.js'

/**
 * Write data as parquet to an ArrayBuffer
 *
 * @import {ColumnChunk, DecodedArray, FileMetaData, SchemaElement, SchemaTree} from 'hyparquet'
 * @import {ColumnData} from '../src/types.js'
 * @param {object} options
 * @param {ColumnData[]} options.columnData
 * @param {boolean} [options.compressed]
 * @returns {ArrayBuffer}
 */
export function parquetWrite({ columnData, compressed = true }) {
  const writer = new Writer()

  // Check if all columns have the same length
  const num_rows = columnData.length ? BigInt(columnData[0].data.length) : 0n
  for (const { data } of columnData) {
    if (BigInt(data.length) !== num_rows) {
      throw new Error('parquetWrite: all columns must have the same length')
    }
  }

  // Write header PAR1
  writer.appendUint32(0x31524150)

  // schema
  /** @type {SchemaElement[]} */
  const schema = [{
    name: 'root',
    num_children: columnData.length,
  }]

  // row group columns
  /** @type {ColumnChunk[]} */
  const columns = []

  // Write columns
  for (const { name, data } of columnData) {
    // auto-detect type
    const schemaElement = getSchemaElementForValues(name, data)
    if (!schemaElement.type) throw new Error(`column ${name} cannot determine type`)
    const file_offset = BigInt(writer.offset)
    /** @type {SchemaElement[]} */
    const schemaPath = [
      schema[0],
      schemaElement,
    ]
    const meta_data = writeColumn(writer, schemaPath, data, compressed)

    // save metadata
    schema.push(schemaElement)
    columns.push({
      file_path: name,
      file_offset,
      meta_data,
    })
  }

  // Write metadata
  /** @type {FileMetaData} */
  const metadata = {
    version: 2,
    created_by: 'hyparquet',
    schema,
    num_rows,
    row_groups: [{
      columns,
      total_byte_size: BigInt(writer.offset - 4),
      num_rows,
    }],
    metadata_length: 0,
  }
  // @ts-ignore don't want to actually serialize metadata_length
  delete metadata.metadata_length
  writeMetadata(writer, metadata)

  // Write footer PAR1
  writer.appendUint32(0x31524150)

  return writer.getBuffer()
}
