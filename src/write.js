import { getSchemaElementForValues } from './schema.js'
import { ParquetWriter } from './parquet-writer.js'

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
  const schema = schemaFromColumnData(columnData)
  const writer = new ParquetWriter({
    schema,
    compressed,
    statistics,
    kvMetadata,
  })

  writer.write({
    columnData,
    rowGroupSize,
  })

  return writer.finish()
}

/**
 * Convert column data to schema.
 *
 * @param {ColumnData[]} columnData
 * @returns {SchemaElement[]}
 */
function schemaFromColumnData(columnData) {
  /** @type {SchemaElement[]} */
  const schema = [{
    name: 'root',
    num_children: columnData.length,
  }]
  let num_rows = 0
  for (const { name, data, type } of columnData) {
    // check if all columns have the same length
    if (num_rows === 0) {
      num_rows = data.length
    } else if (num_rows !== data.length) {
      throw new Error('columns must have the same length')
    }
    // auto-detect type
    const schemaElement = getSchemaElementForValues(name, data, type)
    if (!schemaElement.type) throw new Error(`column ${name} cannot determine type`)
    schema.push(schemaElement)
  }
  return schema
}
