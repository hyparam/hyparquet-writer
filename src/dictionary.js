import { ByteWriter } from './bytewriter.js'
import { writePageHeader } from './datapage.js'
import { writePlain } from './plain.js'

/**
 * @import {DecodedArray, Encoding, ParquetType} from 'hyparquet'
 * @import {ColumnEncoder, Writer} from './types.js'
 */

/**
 * Estimate the byte size of a value for page size calculation.
 *
 * @param {any} value
 * @param {ParquetType} type
 * @param {number} [type_length]
 * @returns {number}
 */
export function estimateValueSize(value, type, type_length) {
  if (value === null || value === undefined) return 0
  if (type === 'BOOLEAN') return 0.125
  if (type === 'INT32' || type === 'FLOAT') return 4
  if (type === 'INT64' || type === 'DOUBLE') return 8
  if (type === 'INT96') return 12
  if (type === 'FIXED_LEN_BYTE_ARRAY') return type_length ?? 0
  if (type === 'BYTE_ARRAY') {
    if (value instanceof Uint8Array) return value.byteLength
    if (typeof value === 'string') return value.length
  }
  return 0
}

/**
 * FNV-1a hash of a byte array, used to bucket byte-array dictionary values
 * without allocating a string key per value.
 *
 * @param {Uint8Array} bytes
 * @returns {number}
 */
function hashBytes(bytes) {
  let h = 0x811c9dc5
  for (let i = 0; i < bytes.length; i++) {
    h ^= bytes[i]
    h = Math.imul(h, 0x01000193)
  }
  return h >>> 0
}

/**
 * @param {Uint8Array} a
 * @param {Uint8Array} b
 * @returns {boolean}
 */
function bytesEqual(a, b) {
  if (a.length !== b.length) return false
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false
  }
  return true
}

/**
 * Decide whether to dictionary-encode a column, and if so build the dictionary
 * and per-row indexes. Returns {} to fall back to plain encoding.
 *
 * @param {DecodedArray} values
 * @param {ParquetType} type
 * @param {number | undefined} type_length
 * @param {Encoding | undefined} encoding
 * @param {number} pageSize
 * @returns {{ dictionary?: any[], indexes?: number[] }}
 */
export function useDictionary(values, type, type_length, encoding, pageSize) {
  if (encoding && encoding !== 'RLE_DICTIONARY') return {}
  if (type === 'BOOLEAN') return {}

  // uniqueness on a sample. Byte arrays are keyed by hash so distinct
  // Uint8Array objects with identical bytes count as one (a plain Set would key
  // them by object identity); null/undefined count as values, matching the
  // plain-encoding fallback that validates required/missing values.
  const sample = values.slice(0, 1000)
  const sampleKeys = new Set()
  for (const value of sample) {
    sampleKeys.add(value instanceof Uint8Array ? hashBytes(value) : value)
  }
  if (sampleKeys.size === 0 || sampleKeys.size / sample.length > 0.5) return {}

  // build dictionary and indexes. Primitives (string/number/bigint) dedupe by
  // value; byte arrays dedupe by content via hash buckets with an exact
  // byte-equality check (hashes can collide).
  /** @type {any[]} */
  const dictionary = []
  /** @type {number[]} */
  const indexes = new Array(values.length)
  /** @type {Map<any, number>} */
  const valueIndex = new Map()
  /** @type {Map<number, number[]>} */
  const hashBuckets = new Map()
  let dictSize = 0
  for (let i = 0; i < values.length; i++) {
    const value = values[i]
    if (value === null || value === undefined) continue

    let index
    if (value instanceof Uint8Array) {
      const hash = hashBytes(value)
      const bucket = hashBuckets.get(hash)
      if (bucket) {
        for (const j of bucket) {
          if (bytesEqual(dictionary[j], value)) { index = j; break }
        }
      }
      if (index === undefined) {
        dictSize += value.byteLength
        if (pageSize && dictSize > pageSize) return {}
        index = dictionary.length
        dictionary.push(value)
        if (bucket) bucket.push(index)
        else hashBuckets.set(hash, [index])
      }
    } else {
      index = valueIndex.get(value)
      if (index === undefined) {
        dictSize += estimateValueSize(value, type, type_length)
        if (pageSize && dictSize > pageSize) return {}
        index = dictionary.length
        dictionary.push(value)
        valueIndex.set(value, index)
      }
    }
    indexes[i] = index
  }

  // TODO: sort by frequency?
  return { dictionary, indexes }
}

/**
 * @param {Writer} writer
 * @param {ColumnEncoder} column
 * @param {DecodedArray} dictionary
 */
export function writeDictionaryPage(writer, column, dictionary) {
  const { element, codec, compressors } = column
  const { type, type_length } = element
  if (!type) throw new Error(`column ${column.columnName} cannot determine type`)

  // write values to temp buffer
  const dictionaryPage = new ByteWriter()
  writePlain(dictionaryPage, dictionary, type, type_length)
  const dictionaryBytes = dictionaryPage.getBytes()

  // compress dictionary page data
  const compressedBytes = compressors[codec]?.(dictionaryBytes) ?? dictionaryBytes

  // write dictionary page header
  writePageHeader(writer, {
    type: 'DICTIONARY_PAGE',
    uncompressed_page_size: dictionaryBytes.byteLength,
    compressed_page_size: compressedBytes.byteLength,
    dictionary_page_header: {
      num_values: dictionary.length,
      encoding: 'PLAIN',
    },
  })
  writer.appendBytes(compressedBytes)
}
