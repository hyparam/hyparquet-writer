import fs from 'fs'
import { ByteWriter } from './bytewriter.js'
import { parquetWrite } from './write.js'

export * from './index.js'

/**
 * Write data as parquet to a local file.
 *
 * @import {ParquetWriteOptions, Writer} from '../src/types.js'
 * @param {Omit<ParquetWriteOptions, 'writer'> & {filename: string}} options
 */
export function parquetWriteFile(options) {
  const { filename, ...rest } = options
  const writer = fileWriter(filename)
  parquetWrite({ ...rest, writer })
}

/**
 * Buffered file writer.
 * Writes data to a local file in chunks using node fs.
 *
 * @param {string} filename
 * @returns {Writer}
 */
export function fileWriter(filename) {
  const writer = new ByteWriter()
  const chunkSize = 1_000_000 // 1mb

  // create a new file or overwrite existing one
  fs.writeFileSync(filename, '', { flag: 'w' })

  function flush() {
    const chunk = writer.buffer.slice(0, writer.index)
    // TODO: async
    fs.writeFileSync(filename, new Uint8Array(chunk), { flag: 'a' })
    writer.index = 0
  }

  /**
   * Override the ensure method
   * @param {number} size
   */
  writer.ensure = function(size) {
    if (writer.index > chunkSize) {
      flush()
    }
    if (writer.index + size > writer.buffer.byteLength) {
      const newSize = Math.max(writer.buffer.byteLength * 2, writer.index + size)
      const newBuffer = new ArrayBuffer(newSize)
      new Uint8Array(newBuffer).set(new Uint8Array(writer.buffer))
      writer.buffer = newBuffer
      writer.view = new DataView(writer.buffer)
    }
  }
  writer.getBuffer = function () {
    throw new Error('getBuffer not supported for FileWriter')
  }
  writer.finish = function() {
    flush()
  }
  return writer
}
