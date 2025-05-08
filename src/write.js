import { ByteWriter } from './bytewriter.js'
import { ParquetWriter } from './parquet-writer.js'
import { schemaFromColumnData } from './schema.js'

/**
 * Write data as parquet to a file or stream.
 *
 * @import {ParquetWriteOptions} from '../src/types.js'
 * @param {ParquetWriteOptions} options
 */
export function parquetWrite({
  writer,
  columnData,
  schema,
  compressed = true,
  statistics = true,
  rowGroupSize = 100000,
  kvMetadata,
}) {
  if (!schema) {
    schema = schemaFromColumnData(columnData)
  } else if (columnData.some(({ type }) => type)) {
    throw new Error('cannot provide both schema and columnData type')
  } else {
    // TODO: validate schema
  }
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
}

/**
 * Write data as parquet to an ArrayBuffer.
 *
 * @param {Omit<ParquetWriteOptions, 'writer'>} options
 * @returns {ArrayBuffer}
 */
export function parquetWriteBuffer(options) {
  const writer = new ByteWriter()
  parquetWrite({ ...options, writer })
  return writer.getBuffer()
}
