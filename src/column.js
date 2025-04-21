import { unconvert } from './unconvert.js'
import { writePlain } from './plain.js'
import { snappyCompress } from './snappy.js'
import { ByteWriter } from './bytewriter.js'
import { writeDataPageV2, writePageHeader } from './datapage.js'

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
  /** @type {Encoding[]} */
  const encodings = []

  // Compute statistics
  const statistics = stats ? getStatistics(values) : undefined

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
    const unconverted = unconvert(schemaElement, dictionary)
    writeDictionaryPage(writer, unconverted, type, compressed)

    // write data page with dictionary indexes
    data_page_offset = BigInt(writer.offset)
    writeDataPageV2(writer, indexes, type, schemaPath, 'RLE_DICTIONARY', compressed)
    encodings.push('RLE_DICTIONARY')
  } else {
    // unconvert values from rich types to simple
    values = unconvert(schemaElement, values)

    // write data page
    const encoding = type === 'BOOLEAN' && values.length > 16 ? 'RLE' : 'PLAIN'
    writeDataPageV2(writer, values, type, schemaPath, encoding, compressed)
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
