import { unconvert } from './unconvert.js'
import { writePlain } from './plain.js'
import { snappyCompress } from './snappy.js'
import { ByteWriter } from './bytewriter.js'
import { writeDataPageV2, writePageHeader } from './datapage.js'

/**
 * @param {import('./types.js').Writer} writer
 * @param {import('./types.js').SchemaElement[]} schemaPath
 * @param {import('./types.js').DecodedArray} values
 * @param {boolean} compressed
 * @param {boolean} stats
 * @returns {import('./types.js').ColumnMetaData}
 */
export function writeColumn(writer, schemaPath, values, compressed, stats) {
  const element = schemaPath[schemaPath.length - 1]
  const { type, type_length } = element
  if (!type) throw new Error(`column ${element.name} cannot determine type`)
  const offsetStart = writer.offset
  const num_values = values.length
  /** @type {import('./types.js').Encoding[]} */
  const encodings = []

  // Compute statistics
  const statistics = stats ? getStatistics(values) : undefined

  // dictionary encoding
  let dictionary_page_offset
  let data_page_offset = BigInt(writer.offset)
  /** @type {import('./types.js').DecodedArray | undefined} */
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
    writeDictionaryPage(writer, unconverted, type, type_length, compressed)

    // write data page with dictionary indexes
    data_page_offset = BigInt(writer.offset)
    writeDataPageV2(writer, indexes, schemaPath, 'RLE_DICTIONARY', compressed)
    encodings.push('RLE_DICTIONARY')
  } else {
    // unconvert values from rich types to simple
    values = unconvert(element, values)

    // write data page
    const encoding = type === 'BOOLEAN' && values.length > 16 ? 'RLE' : 'PLAIN'
    writeDataPageV2(writer, values, schemaPath, encoding, compressed)
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
 * @param {import('./types.js').DecodedArray} values
 * @param {import('./types.js').ParquetType} type
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
 * @param {import('./types.js').Writer} writer
 * @param {import('./types.js').DecodedArray} dictionary
 * @param {import('./types.js').ParquetType} type
 * @param {number | undefined} fixedLength
 * @param {boolean} compressed
 */
function writeDictionaryPage(writer, dictionary, type, fixedLength, compressed) {
  const dictionaryPage = new ByteWriter()
  writePlain(dictionaryPage, dictionary, type, fixedLength)

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
 * @param {import('./types.js').DecodedArray} values
 * @returns {import('./types.js').Statistics}
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
