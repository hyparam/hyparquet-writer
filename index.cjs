'use strict'

const modulePromise = import('./src/index.js')

module.exports = {
  /**
   * @param {ParquetWriteOptions} options
   * @returns {Promise<void>}
   */
  parquetWrite: async function (options) {
    const module = await modulePromise
    return module.parquetWrite(options)
  },
  /**
   * @param {Omit<ParquetWriteOptions, "writer">} options
   * @returns {Promise<ArrayBuffer>}
   */
  parquetWriteBuffer: async function (options) {
    const module = await modulePromise
    return module.parquetWriteBuffer(options)
  },
  /**
   * @param {Omit<ParquetWriteOptions, 'writer'> & {filename: string}} options
   * @returns {Promise<void>}
   */
  parquetWriteFile: async function (options) {
    const module = await modulePromise
    return module.parquetWriteFile(options)
  },
  /**
   * @returns {Promise<ParquetWriter>}
   */
  getParquetWriter: async function () {
    const module = await modulePromise
    return module.ParquetWriter
  },
  /**
   * @returns {Promise<ByteWriter>}
   */
  getByteWriter: async function () {
    const module = await modulePromise
    return module.ByteWriter
  },
}
