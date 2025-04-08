import { Encoding, PageType } from 'hyparquet/src/constants.js'
import { unconvert } from './unconvert.js'
import { writeRleBitPackedHybrid } from './encoding.js'
import { writePlain } from './plain.js'
import { getMaxDefinitionLevel, getMaxRepetitionLevel } from './schema.js'
import { snappyCompress } from './snappy.js'
import { serializeTCompactProtocol } from './thrift.js'
import { ByteWriter } from './bytewriter.js'

/**
 * @import {ColumnMetaData, DecodedArray, PageHeader, ParquetType, SchemaElement, Statistics} from 'hyparquet'
 * @import {Writer} from '../src/types.js'
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
  let dataType = type
  const offsetStart = writer.offset
  const num_values = values.length
  /** @type {Statistics | undefined} */
  let statistics = undefined

  // Compute statistics
  if (stats) {
    statistics = {
      min_value: undefined,
      max_value: undefined,
      null_count: 0n,
    }
    let null_count = 0n
    for (const value of values) {
      if (value === null || value === undefined) {
        null_count++
        continue
      }
      if (statistics.min_value === undefined || value < statistics.min_value) {
        statistics.min_value = value
      }
      if (statistics.max_value === undefined || value > statistics.max_value) {
        statistics.max_value = value
      }
    }
    statistics.null_count = null_count
  }

  // Write levels to temp buffer
  const levels = new ByteWriter()
  const { definition_levels_byte_length, repetition_levels_byte_length, num_nulls } = writeLevels(levels, schemaPath, values)

  // dictionary encoding
  let dictionary_page_offset = undefined
  /** @type {DecodedArray | undefined} */
  let dictionary = useDictionary(values, dataType)
  if (dictionary) {
    dictionary_page_offset = BigInt(writer.offset)

    // replace values with dictionary indices
    const indexes = new Int32Array(values.length)
    for (let i = 0; i < values.length; i++) {
      indexes[i] = dictionary.indexOf(values[i])
    }
    values = indexes
    dataType = 'INT32'

    // unconvert dictionary and filter out nulls
    dictionary = unconvert(schemaElement, dictionary)
      .filter(v => v !== null && v !== undefined)

    // write dictionary page data
    writeDictionaryPage(writer, dictionary, type, compressed)
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
 * @param {Writer} writer
 * @param {PageHeader} header
 */
function writePageHeader(writer, header) {
  const compact = {
    field_1: PageType.indexOf(header.type),
    field_2: header.uncompressed_page_size,
    field_3: header.compressed_page_size,
    field_7: header.dictionary_page_header && {
      field_1: header.dictionary_page_header.num_values,
      field_2: Encoding.indexOf(header.dictionary_page_header.encoding),
    },
    field_8: header.data_page_header_v2 && {
      field_1: header.data_page_header_v2.num_values,
      field_2: header.data_page_header_v2.num_nulls,
      field_3: header.data_page_header_v2.num_rows,
      field_4: Encoding.indexOf(header.data_page_header_v2.encoding),
      field_5: header.data_page_header_v2.definition_levels_byte_length,
      field_6: header.data_page_header_v2.repetition_levels_byte_length,
      field_7: header.data_page_header_v2.is_compressed ? undefined : false, // default true
    },
  }
  serializeTCompactProtocol(writer, compact)
}

/**
 * @param {DecodedArray} values
 * @param {ParquetType} type
 * @returns {any[] | undefined}
 */
function useDictionary(values, type) {
  if (type === 'BOOLEAN') return
  const unique = new Set(values)
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
 * @param {Writer} writer
 * @param {SchemaElement[]} schemaPath
 * @param {DecodedArray} values
 * @returns {{ definition_levels_byte_length: number, repetition_levels_byte_length: number, num_nulls: number}}
 */
function writeLevels(writer, schemaPath, values) {
  let num_nulls = 0

  // TODO: repetition levels
  const maxRepetitionLevel = getMaxRepetitionLevel(schemaPath)
  let repetition_levels_byte_length = 0
  if (maxRepetitionLevel) {
    repetition_levels_byte_length = writeRleBitPackedHybrid(writer, [])
  }

  // definition levels
  const maxDefinitionLevel = getMaxDefinitionLevel(schemaPath)
  let definition_levels_byte_length = 0
  if (maxDefinitionLevel) {
    const definitionLevels = []
    for (const value of values) {
      if (value === null || value === undefined) {
        definitionLevels.push(maxDefinitionLevel - 1)
        num_nulls++
      } else {
        definitionLevels.push(maxDefinitionLevel)
      }
    }
    definition_levels_byte_length = writeRleBitPackedHybrid(writer, definitionLevels)
  }
  return { definition_levels_byte_length, repetition_levels_byte_length, num_nulls }
}
