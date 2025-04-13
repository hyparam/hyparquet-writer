import { unconvert } from './unconvert.js'
import { writeRleBitPackedHybrid } from './encoding.js'
import { writePlain } from './plain.js'
import { snappyCompress } from './snappy.js'
import { ByteWriter } from './bytewriter.js'
import { writeLevels, writePageHeader } from './datapage.js'

/**
 * @param {Writer} writer
 * @param {SchemaElement[]} schemaPath
 * @param {DecodedArray} values
 * @param {boolean} compressed
 * @param {boolean} stats
 * @returns {ColumnMetaData}
 */
export function writeColumn(writer, schemaPath, values, compressed, stats) {
  const schemaElement = schemaPath[schemaPath.length - 1]
  const { type } = schemaElement
  if (!type) throw new Error(`column ${schemaElement.name} cannot determine type`)
  const offsetStart = writer.offset
  const num_values = values.length

  // Compute statistics
  const statistics = stats ? getStatistics(values) : undefined

  // Write levels to temp buffer
  const levels = new ByteWriter()
  const { definition_levels_byte_length, repetition_levels_byte_length, num_nulls }
    = writeLevels(levels, schemaPath, values)

  // dictionary encoding
  let dictionary_page_offset = undefined
  /** @type {DecodedArray | undefined} */
  const dictionary = useDictionary(values, type)
  if (dictionary) {
    dictionary_page_offset = BigInt(writer.offset)

    // replace values with dictionary indices
    const indexes = new Int32Array(values.length)
    for (let i = 0; i < values.length; i++) {
      indexes[i] = dictionary.indexOf(values[i])
    }
    values = indexes

    // write unconverted dictionary page
    const unconverted = unconvert(schemaElement, dictionary)
    writeDictionaryPage(writer, unconverted, type, compressed)
  } else {
    // unconvert type and filter out nulls
    values = unconvert(schemaElement, values)
      .filter(v => v !== null && v !== undefined)
  }

  // write page data to temp buffer
  const page = new ByteWriter()
  /** @type {import('hyparquet').Encoding} */
  const encoding = dictionary ? 'RLE_DICTIONARY' : 'PLAIN'
  if (dictionary) {
    const bitWidth = Math.ceil(Math.log2(dictionary.length))
    page.appendUint8(bitWidth)
    writeRleBitPackedHybrid(page, values)
  } else {
    writePlain(page, values, type)
  }

  // compress page data
  let compressedPage = page
  if (compressed) {
    compressedPage = new ByteWriter()
    snappyCompress(compressedPage, new Uint8Array(page.getBuffer()))
  }

  // write page header
  const data_page_offset = BigInt(writer.offset)
  /** @type {PageHeader} */
  const header = {
    type: 'DATA_PAGE_V2',
    uncompressed_page_size: levels.offset + page.offset,
    compressed_page_size: levels.offset + compressedPage.offset,
    data_page_header_v2: {
      num_values,
      num_nulls,
      num_rows: num_values,
      encoding: dictionary ? 'RLE_DICTIONARY' : encoding,
      definition_levels_byte_length,
      repetition_levels_byte_length,
      is_compressed: true,
    },
  }
  writePageHeader(writer, header)

  // write levels
  writer.appendBuffer(levels.getBuffer())

  // write page data
  writer.appendBuffer(compressedPage.getBuffer())

  return {
    type,
    encodings: [encoding],
    path_in_schema: schemaPath.slice(1).map(s => s.name),
    codec: compressed ? 'SNAPPY' : 'UNCOMPRESSED',
    num_values: BigInt(num_values),
    total_compressed_size: BigInt(writer.offset - offsetStart),
    total_uncompressed_size: BigInt(writer.offset - offsetStart),
    data_page_offset,
    dictionary_page_offset,
    statistics,
  }
}

/**
 * @param {DecodedArray} values
 * @param {ParquetType} type
 * @returns {any[] | undefined}
 */
function useDictionary(values, type) {
  if (type === 'BOOLEAN') return
  const unique = new Set(values)
  unique.delete(undefined)
  unique.delete(null)
  if (values.length > 10 && values.length / unique.size > 0.1) {
    if (unique.size < values.length) {
      // TODO: sort by frequency
      return Array.from(unique)
    }
  }
}

/**
 * @param {Writer} writer
 * @param {DecodedArray} dictionary
 * @param {ParquetType} type
 * @param {boolean} compressed
 */
function writeDictionaryPage(writer, dictionary, type, compressed) {
  const dictionaryPage = new ByteWriter()
  writePlain(dictionaryPage, dictionary, type)

  // compress dictionary page data
  let compressedDictionaryPage = dictionaryPage
  if (compressed) {
    compressedDictionaryPage = new ByteWriter()
    snappyCompress(compressedDictionaryPage, new Uint8Array(dictionaryPage.getBuffer()))
  }

  // write dictionary page header
  /** @type {PageHeader} */
  const dictionaryHeader = {
    type: 'DICTIONARY_PAGE',
    uncompressed_page_size: dictionaryPage.offset,
    compressed_page_size: compressedDictionaryPage.offset,
    dictionary_page_header: {
      num_values: dictionary.length,
      encoding: 'PLAIN',
    },
  }
  writePageHeader(writer, dictionaryHeader)
  writer.appendBuffer(compressedDictionaryPage.getBuffer())
}

/**
 * @import {ColumnMetaData, DecodedArray, PageHeader, ParquetType, SchemaElement, Statistics} from 'hyparquet'
 * @import {Writer} from '../src/types.js'
 * @param {DecodedArray} values
 * @returns {Statistics}
 */
function getStatistics(values) {
  let min_value = undefined
  let max_value = undefined
  let null_count = 0n
  for (const value of values) {
    if (value === null || value === undefined) {
      null_count++
      continue
    }
    if (min_value === undefined || value < min_value) {
      min_value = value
    }
    if (max_value === undefined || value > max_value) {
      max_value = value
    }
  }
  return { min_value, max_value, null_count }
}
