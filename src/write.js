import { getParquetTypeForValues, writeColumn } from './column.js'
import { Writer } from './writer.js'
import { writeMetadata } from './metadata.js'

/**
 * Write data as parquet to an ArrayBuffer
 *
 * @import {ColumnChunk, DecodedArray, FileMetaData, SchemaElement, SchemaTree} from 'hyparquet'
 * @param {Record<string, DecodedArray>} columnData
 * @returns {ArrayBuffer}
 */
export function parquetWrite(columnData) {
  const writer = new Writer()

  // Check if all columns have the same length
  const columnNames = Object.keys(columnData)
  const num_rows = columnNames.length ? BigInt(columnData[columnNames[0]].length) : 0n
  for (const name of columnNames) {
    if (BigInt(columnData[name].length) !== num_rows) {
      throw new Error('parquetWrite: all columns must have the same length')
    }
  }

  // Write header PAR1
  writer.appendUint32(0x31524150)

  // schema
  /** @type {SchemaElement[]} */
  const schema = [{
    name: 'root',
    num_children: columnNames.length,
  }]

  // row group columns
  /** @type {ColumnChunk[]} */
  const columns = []

  // Write columns
  for (const name of columnNames) {
    const values = columnData[name]
    const { type, repetition_type } = getParquetTypeForValues(values)
    if (!type) throw new Error(`parquetWrite: empty column ${name} cannot determine type`)
    const file_offset = BigInt(writer.offset)
    /** @type {SchemaElement[]} */
    const schemaElements = [
      schema[0],
      { type, name, repetition_type, num_children: 0 },
    ]
    const meta_data = writeColumn(writer, schemaElements, values, type)

    // save metadata
    schema.push({ type, name, repetition_type })
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
