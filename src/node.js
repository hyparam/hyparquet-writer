import { fileWriter } from './filewriter.js'
import { parquetWrite } from './write.js'

export { parquetWrite, parquetWriteBuffer } from './write.js'
export { ByteWriter } from './bytewriter.js'
export { ParquetWriter } from './parquet-writer.js'
export { fileWriter }

/**
 * Write data as parquet to a local file.
 *
 * @import {ParquetWriteOptions} from '../src/types.js'
 * @param {Omit<ParquetWriteOptions, 'writer'> & {filename: string}} options
 */
export function parquetWriteFile(options) {
  const { filename, ...rest } = options
  const writer = fileWriter(filename)
  parquetWrite({ ...rest, writer })
}
