import { ByteWriter } from './bytewriter.js'
import { writeDataPageV2, writePageHeader } from './datapage.js'
import { encodeListValues } from './dremel.js'
import { geospatialStatistics } from './geospatial.js'
import { writePlain } from './plain.js'
import { snappyCompress } from './snappy.js'
import { unconvert } from './unconvert.js'

/**
 * Write a column chunk to the writer.
 *
 * @param {object} options
 * @param {Writer} options.writer
 * @param {ColumnEncoder} options.column
 * @param {DecodedArray} options.values
 * @param {boolean} options.stats
 * @param {number} options.pageSize
 * @returns {ColumnChunk}
 */
export function writeColumn({ writer, column, values, stats, pageSize }) {
  const { columnName, element, schemaPath, compressed, encoding: userEncoding } = column
  const { type, type_length } = element
  if (!type) throw new Error(`column ${columnName} cannot determine type`)
  const offsetStart = writer.offset

  /** @type {PageData | undefined} */
  let pageData
  if (isListLike(schemaPath)) {
    if (!Array.isArray(values)) {
      throw new Error(`parquet column ${columnName} expects array values for list encoding`)
    }
    pageData = encodeListValues(schemaPath, values)
    values = pageData.values
  }

  const num_values = values.length
  /** @type {Encoding[]} */
  const encodings = []

  const isGeospatial = element?.logical_type?.type === 'GEOMETRY' || element?.logical_type?.type === 'GEOGRAPHY'

  // Compute statistics
  const statistics = stats ? getStatistics(values) : undefined
  const geospatial_statistics = stats && isGeospatial ? geospatialStatistics(values) : undefined

  // dictionary encoding
  let dictionary_page_offset
  let data_page_offset = BigInt(writer.offset)
  const dictionary = useDictionary(values, type, userEncoding)

  // Determine encoding and prepare values for writing
  /** @type {Encoding} */
  let encoding
  let writeValues
  if (dictionary) {
    // replace values with dictionary indices
    const indexes = new Array(values.length)
    for (let i = 0; i < values.length; i++) {
      if (values[i] !== null && values[i] !== undefined) {
        indexes[i] = dictionary.indexOf(values[i])
      }
    }
    writeValues = indexes
    encoding = 'RLE_DICTIONARY'

    // write dictionary page first
    dictionary_page_offset = BigInt(writer.offset)
    const unconverted = unconvert(element, dictionary)
    writeDictionaryPage(writer, column, unconverted)
  } else {
    // unconvert values from rich types to simple
    writeValues = unconvert(element, values)
    encoding = userEncoding ?? (type === 'BOOLEAN' && values.length > 16 ? 'RLE' : 'PLAIN')
  }
  encodings.push(encoding)

  // Split values into pages based on pageSize
  const pageChunks = splitIntoPages(writeValues, type, type_length, pageSize, pageData)

  // Write data pages
  data_page_offset = BigInt(writer.offset)
  for (const chunk of pageChunks) {
    writeDataPageV2(writer, chunk.values, column, encoding, chunk.pageData)
  }

  return {
    meta_data: {
      type,
      encodings,
      path_in_schema: schemaPath.slice(1).map(s => s.name),
      codec: compressed ? 'SNAPPY' : 'UNCOMPRESSED',
      num_values: BigInt(num_values),
      total_compressed_size: BigInt(writer.offset - offsetStart),
      total_uncompressed_size: BigInt(writer.offset - offsetStart), // TODO
      data_page_offset,
      dictionary_page_offset,
      statistics,
      geospatial_statistics,
    },
    file_offset: BigInt(offsetStart),
  }
}

/**
 * Split values into page chunks based on estimated byte size.
 *
 * @param {DecodedArray} values
 * @param {ParquetType} type
 * @param {number | undefined} type_length
 * @param {number | undefined} pageSize
 * @param {PageData | undefined} pageData
 * @returns {Array<{values: DecodedArray, pageData: PageData | undefined}>}
 */
function splitIntoPages(values, type, type_length, pageSize, pageData) {
  // If no pageSize limit, return single page with all values
  if (!pageSize) {
    return [{ values, pageData }]
  }

  const chunks = []
  let pageStart = 0
  let accumulatedBytes = 0

  for (let i = 0; i < values.length; i++) {
    const valueSize = estimateValueSize(values[i], type, type_length)
    accumulatedBytes += valueSize

    // Check if we should start a new page
    if (accumulatedBytes >= pageSize && i > pageStart) {
      chunks.push(createPageChunk(values, pageData, pageStart, i))
      pageStart = i
      accumulatedBytes = valueSize
    }
  }

  // Final page with remaining values
  if (pageStart < values.length) {
    chunks.push(createPageChunk(values, pageData, pageStart, values.length))
  }

  return chunks
}

/**
 * Create a page chunk with sliced values and pageData.
 *
 * @param {DecodedArray} values
 * @param {PageData | undefined} pageData
 * @param {number} start
 * @param {number} end
 * @returns {{values: DecodedArray, pageData: PageData | undefined}}
 */
function createPageChunk(values, pageData, start, end) {
  const chunkValues = values.slice(start, end)
  if (!pageData) {
    return { values: chunkValues, pageData: undefined }
  }
  const defLevels = pageData.definitionLevels.slice(start, end)
  const maxDefLevel = Math.max(...pageData.definitionLevels)
  return {
    values: chunkValues,
    pageData: {
      values: chunkValues,
      definitionLevels: defLevels,
      repetitionLevels: pageData.repetitionLevels.slice(start, end),
      numNulls: defLevels.filter(level => level < maxDefLevel).length,
    },
  }
}

/**
 * Estimate the byte size of a value for page size calculation.
 *
 * @param {any} value
 * @param {ParquetType} type
 * @param {number | undefined} type_length
 * @returns {number}
 */
function estimateValueSize(value, type, type_length) {
  if (value === null || value === undefined) return 0
  if (type === 'BOOLEAN') return 1 // bit, but count as byte for simplicity
  if (type === 'INT32' || type === 'FLOAT') return 4
  if (type === 'INT64' || type === 'DOUBLE') return 8
  if (type === 'INT96') return 12
  if (type === 'FIXED_LEN_BYTE_ARRAY') return type_length ?? 0
  if (type === 'BYTE_ARRAY') {
    if (value instanceof Uint8Array) return value.byteLength
    if (typeof value === 'string') return value.length
    return 0
  }
  return 4 // conservative default
}

/**
 * @param {DecodedArray} values
 * @param {ParquetType} type
 * @param {Encoding | undefined} encoding
 * @returns {any[] | undefined}
 */
function useDictionary(values, type, encoding) {
  if (encoding && encoding !== 'RLE_DICTIONARY') return
  if (type === 'BOOLEAN') return
  const unique = new Set(values)
  unique.delete(undefined)
  unique.delete(null)
  if (values.length / unique.size > 2) {
    // TODO: sort by frequency
    return Array.from(unique)
  }
}

/**
 * @param {Writer} writer
 * @param {ColumnEncoder} column
 * @param {DecodedArray} dictionary
 */
function writeDictionaryPage(writer, column, dictionary) {
  const { element, compressed } = column
  const { type, type_length } = element
  if (!type) throw new Error(`column ${column.columnName} cannot determine type`)
  const dictionaryPage = new ByteWriter()
  writePlain(dictionaryPage, dictionary, type, type_length)

  // compress dictionary page data
  let compressedDictionaryPage = dictionaryPage
  if (compressed) {
    compressedDictionaryPage = new ByteWriter()
    snappyCompress(compressedDictionaryPage, new Uint8Array(dictionaryPage.getBuffer()))
  }

  // write dictionary page header
  writePageHeader(writer, {
    type: 'DICTIONARY_PAGE',
    uncompressed_page_size: dictionaryPage.offset,
    compressed_page_size: compressedDictionaryPage.offset,
    dictionary_page_header: {
      num_values: dictionary.length,
      encoding: 'PLAIN',
    },
  })
  writer.appendBuffer(compressedDictionaryPage.getBuffer())
}

/**
 * @import {ColumnChunk, DecodedArray, Encoding, ParquetType, SchemaElement, Statistics} from 'hyparquet'
 * @import {ColumnEncoder, PageData, Writer} from '../src/types.js'
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
    if (typeof value === 'object') continue // skip objects
    if (min_value === undefined || value < min_value) min_value = value
    if (max_value === undefined || value > max_value) max_value = value
  }
  return { min_value, max_value, null_count }
}

/**
 * @param {SchemaElement[]} schemaPath
 * @returns {boolean}
 */
function isListLike(schemaPath) {
  for (let i = 1; i < schemaPath.length; i++) {
    const element = schemaPath[i]
    if (element?.converted_type === 'LIST') {
      const repeatedChild = schemaPath[i + 1]
      return repeatedChild?.repetition_type === 'REPEATED'
    }
  }
  return false
}
