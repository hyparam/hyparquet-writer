"use strict";

const { pathToFileURL } = require("url");
const path = require("path");

async function importModule(relativePath) {
  const moduleUrl = pathToFileURL(path.join(__dirname, relativePath)).href;
  return import(moduleUrl);
}

module.exports = {
  async getParquetWriter() {
    const module = await importModule("src/parquet-writer.js");
    return module.ParquetWriter;
  },

  async getByteWriter() {
    const module = await importModule("src/bytewriter.js");
    return module.ByteWriter;
  },

  async parquetWrite(options) {
    const module = await importModule("src/write.js");
    return module.parquetWrite(options);
  },

  async parquetWriteBuffer(options) {
    const module = await importModule("src/write.js");
    return module.parquetWriteBuffer(options);
  },

  async parquetWriteFile(options) {
    const nodeModule = await importModule("src/node.js");
    const writeModule = await importModule("src/write.js");

    const { filename, ...rest } = options;
    const writer = nodeModule.fileWriter(filename);

    return writeModule.parquetWrite({ ...rest, writer });
  },
};
