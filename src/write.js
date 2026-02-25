import { ByteWriter } from './bytewriter.js'
import { ParquetWriter } from './parquet-writer.js'
import { schemaFromColumnData } from './schema.js'

/**
 * @import {ParquetWriteOptions} from '../src/types.js'
 */

/**
 * Write data as parquet to a file or stream.
 *
 * @param {ParquetWriteOptions} options
 */
export function parquetWrite({
  writer,
  columnData,
  schema,
  codec = 'SNAPPY',
  compressors,
  statistics = true,
  rowGroupSize = [1000, 100000],
  kvMetadata,
  pageSize = 1048576,
}) {
  if (!schema) {
    schema = schemaFromColumnData({ columnData })
  } else if (columnData.some(({ type }) => type)) {
    throw new Error('cannot provide both schema and columnData type')
  } else {
    // TODO: validate schema
  }
  const pq = new ParquetWriter({
    writer,
    schema,
    codec,
    compressors,
    statistics,
    kvMetadata,
  })
  pq.write({
    columnData,
    rowGroupSize,
    pageSize,
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
