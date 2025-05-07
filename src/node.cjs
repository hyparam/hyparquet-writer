"use strict";

const { pathToFileURL } = require("url");
const path = require("path");

async function importModule(relativePath) {
  const moduleUrl = pathToFileURL(path.join(__dirname, relativePath)).href;
  return import(moduleUrl);
}

async function getModules() {
  const filewriterModule = await importModule("src/filewriter.js");
  const writeModule = await importModule("src/write.js");
  const bytewriterModule = await importModule("src/bytewriter.js");
  const parquetWriterModule = await importModule("src/parquet-writer.js");

  return {
    fileWriter: filewriterModule.fileWriter,
    parquetWrite: writeModule.parquetWrite,
    parquetWriteBuffer: writeModule.parquetWriteBuffer,
    ByteWriter: bytewriterModule.ByteWriter,
    ParquetWriter: parquetWriterModule.ParquetWriter,
  };
}

module.exports = {
  async getFileWriter() {
    const { fileWriter } = await getModules();
    return fileWriter;
  },

  async parquetWrite(options) {
    const { parquetWrite } = await getModules();
    return parquetWrite(options);
  },

  async parquetWriteBuffer(options) {
    const { parquetWriteBuffer } = await getModules();
    return parquetWriteBuffer(options);
  },

  async getByteWriter() {
    const { ByteWriter } = await getModules();
    return ByteWriter;
  },

  async getParquetWriter() {
    const { ParquetWriter } = await getModules();
    return ParquetWriter;
  },

  async parquetWriteFile(options) {
    const { fileWriter, parquetWrite } = await getModules();
    const { filename, ...rest } = options;
    const writer = fileWriter(filename);
    return parquetWrite({ ...rest, writer });
  },
};
