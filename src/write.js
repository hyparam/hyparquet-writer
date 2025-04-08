import { ParquetWriter } from './parquet-writer.js'
import { schemaFromColumnData } from './schema.js'
import { ByteWriter } from './bytewriter.js'

/**
 * Write data as parquet to an ArrayBuffer
 *
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
  const writer = new ByteWriter()
  const pq = new ParquetWriter({
    writer,
    schema,
    compressed,
    statistics,
    kvMetadata,
  })
  pq.write({
    columnData,
    rowGroupSize,
  })
  pq.finish()
  return writer.getBuffer()
}
