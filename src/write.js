import { ByteWriter } from './bytewriter.js'
import { ParquetWriter } from './parquet-writer.js'
import { schemaFromColumnData } from './schema.js'
import { autoDetectShredding, normalizeShreddingConfig } from './variant.js'

/**
 * @import {ParquetWriteOptions} from '../src/types.js'
 */

/**
 * Write data as parquet to a file or stream.
 *
 * @param {ParquetWriteOptions} options
 * @returns {void | Promise<void>}
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
  // Resolve shredding: true -> auto-detected config
  columnData = columnData.map(col => {
    if (col.shredding === true && col.type === 'VARIANT') {
      const detected = autoDetectShredding(Array.from(col.data))
      return detected ? { ...col, shredding: detected } : { ...col, shredding: undefined }
    }
    if (typeof col.shredding === 'object' && col.type === 'VARIANT') {
      const shredding = normalizeShreddingConfig(col.shredding)
      return shredding ? { ...col, shredding } : { ...col, shredding: undefined }
    }
    return col
  })
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
  const w = pq.write({
    columnData,
    rowGroupSize,
    pageSize,
  })
  return w ? w.then(() => pq.finish()) : pq.finish()
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
