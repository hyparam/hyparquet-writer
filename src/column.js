import { ByteWriter } from './bytewriter.js'
import { writeDataPageV2, writePageHeader } from './datapage.js'
import { encodeListValues } from './dremel.js'
import { geospatialStatistics } from './geospatial.js'
import { writePlain } from './plain.js'
import { snappyCompress } from './snappy.js'
import { unconvert } from './unconvert.js'

/**
 * @param {Writer} writer
 * @param {ColumnEncoder} column
 * @param {DecodedArray} values
 * @param {boolean} stats
 * @returns {ColumnMetaData}
 */
export function writeColumn(writer, column, values, stats) {
  const { columnName, element, schemaPath, compressed } = column
  const { type } = element
  if (!type) throw new Error(`column ${columnName} cannot determine type`)
  const offsetStart = writer.offset

  /** @type {ListValues | undefined} */
  let listValues
  if (isListLike(schemaPath)) {
    if (!Array.isArray(values)) {
      throw new Error(`parquet column ${columnName} expects array values for list encoding`)
    }
    listValues = encodeListValues(schemaPath, values)
    values = listValues.values
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
  /** @type {DecodedArray | undefined} */
  const dictionary = useDictionary(values, type)
  if (dictionary) {
    dictionary_page_offset = BigInt(writer.offset)

    // replace values with dictionary indices
    const indexes = new Array(values.length)
    for (let i = 0; i < values.length; i++) {
      if (values[i] !== null && values[i] !== undefined) {
        indexes[i] = dictionary.indexOf(values[i])
      }
    }

    // write unconverted dictionary page
    const unconverted = unconvert(element, dictionary)
    writeDictionaryPage(writer, column, unconverted)

    // write data page with dictionary indexes
    data_page_offset = BigInt(writer.offset)
    writeDataPageV2(writer, indexes, column, 'RLE_DICTIONARY', listValues)
    encodings.push('RLE_DICTIONARY')
  } else {
    // unconvert values from rich types to simple
    values = unconvert(element, values)

    // write data page
    const encoding = type === 'BOOLEAN' && values.length > 16 ? 'RLE' : 'PLAIN'
    writeDataPageV2(writer, values, column, encoding, listValues)
    encodings.push(encoding)
  }

  return {
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
 * @import {ColumnMetaData, DecodedArray, Encoding, ParquetType, SchemaElement, Statistics} from 'hyparquet'
 * @import {ColumnEncoder, ListValues, Writer} from '../src/types.js'
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
