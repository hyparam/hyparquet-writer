import { ByteWriter } from './bytewriter.js'
import { writePageHeader } from './datapage.js'
import { writePlain } from './plain.js'

const textEncoder = new TextEncoder()

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

// Sampling only rejects columns that are very likely to lose. Columns in the
// uncertain middle are decided by the complete-column win check below.
const sampleRejectionRatio = 0.9

/**
 * Decide whether to dictionary-encode a column, and if so build the dictionary
 * and per-row indexes. Returns {} to fall back to plain encoding.
 *
 * Sampling is spread across the complete column chunk and weighted by value
 * bytes. It only rejects columns whose sampled distinct bytes exceed 90% of
 * sampled value bytes. Other columns are built and kept when distinct-value
 * bytes are at most half the total value bytes, so dictionary encoding offers
 * a material win. There is no fixed size cap by default; a low-cardinality
 * column of large values is exactly where a big dictionary pays for itself,
 * and capping it at page size caused 20-100x file bloat (#35). Callers can
 * still impose a hard cap via `dictionarySize`.
 *
 * When `encoding` explicitly requests RLE_DICTIONARY, the dictionary is built
 * unconditionally (no sampling, no size or win checks), so the written pages
 * always match the requested encoding.
 *
 * @param {DecodedArray} values
 * @param {ParquetType} type
 * @param {number | undefined} type_length
 * @param {Encoding | undefined} encoding
 * @param {number} [dictionarySize] - optional hard cap on distinct-value bytes
 * @param {boolean} [required] - reject null or undefined values
 * @returns {{ dictionary?: any[], indexes?: number[] }}
 */
export function useDictionary(values, type, type_length, encoding, dictionarySize, required) {
  if (encoding && encoding !== 'RLE_DICTIONARY') return {}
  if (type === 'BOOLEAN') return {}
  const forced = encoding === 'RLE_DICTIONARY'
  const byteArrayPrefixSize = type === 'BYTE_ARRAY' ? 4 : 0

  // Estimate distinct-value bytes from a sample spread over the complete
  // column. Byte arrays are keyed by hash so distinct Uint8Array objects with
  // identical bytes count as one (a plain Set would key them by object
  // identity). Null/undefined contribute no encoded value bytes.
  if (!forced) {
    const sampleSize = Math.min(values.length, 1000)
    const sampleSizes = new Map()
    let sampleDictionarySize = 0
    let sampleTotalSize = 0
    for (let i = 0; i < sampleSize; i++) {
      const sampleIndex = sampleSize === 1 ? 0 : Math.floor(i * (values.length - 1) / (sampleSize - 1))
      const value = values[sampleIndex]
      const key = value instanceof Uint8Array ? hashBytes(value) : value
      let valueSize = sampleSizes.get(key)
      if (valueSize === undefined) {
        valueSize = estimateValueSize(value, type, type_length)
          + (value === null || value === undefined ? 0 : byteArrayPrefixSize)
        sampleSizes.set(key, valueSize)
        sampleDictionarySize += valueSize
      }
      sampleTotalSize += valueSize
    }
    if (!sampleTotalSize || sampleDictionarySize / sampleTotalSize > sampleRejectionRatio) return {}
  }

  // build dictionary and indexes. Primitives (string/number/bigint) dedupe by
  // value; byte arrays dedupe by content via hash buckets with an exact
  // byte-equality check (hashes can collide).
  /** @type {any[]} */
  const dictionary = []
  /** @type {number[]} */
  const indexes = new Array(values.length)
  /** @type {Map<any, number>} */
  const valueIndex = new Map()
  /** @type {number[]} */
  const valueSizes = []
  /** @type {Map<number, number[]>} */
  const hashBuckets = new Map()
  let dictSize = 0
  let physicalDictSize = 0
  let totalSize = 0
  let nonNullCount = 0
  for (let i = 0; i < values.length; i++) {
    const value = values[i]
    if (value === null || value === undefined) {
      if (required) throw new Error('parquet required value is undefined')
      continue
    }
    nonNullCount++

    let index
    if (value instanceof Uint8Array) {
      totalSize += value.byteLength
      const hash = hashBytes(value)
      const bucket = hashBuckets.get(hash)
      if (bucket) {
        for (const j of bucket) {
          if (bytesEqual(dictionary[j], value)) { index = j; break }
        }
      }
      if (index === undefined) {
        dictSize += value.byteLength
        if (!forced && dictionarySize) {
          physicalDictSize += value.byteLength
          if (physicalDictSize > dictionarySize) return {}
        }
        index = dictionary.length
        dictionary.push(value)
        if (bucket) bucket.push(index)
        else hashBuckets.set(hash, [index])
      }
    } else {
      index = valueIndex.get(value)
      const valueSize = index === undefined
        ? estimateValueSize(value, type, type_length)
        : valueSizes[index]
      totalSize += valueSize
      if (index === undefined) {
        dictSize += valueSize
        if (!forced && dictionarySize) {
          physicalDictSize += typeof value === 'string' ? textEncoder.encode(value).byteLength : valueSize
          if (physicalDictSize > dictionarySize) return {}
        }
        index = dictionary.length
        dictionary.push(value)
        valueSizes.push(valueSize)
        valueIndex.set(value, index)
      }
    }
    indexes[i] = index
  }

  // An automatic dictionary plus its bit-packed indexes must at least halve
  // the PLAIN value bytes. BYTE_ARRAY values have a four-byte length prefix
  // per occurrence in PLAIN and per distinct value in the dictionary.
  const bitWidth = Math.ceil(Math.log2(dictionary.length))
  const indexSize = Math.ceil(nonNullCount * bitWidth / 8)
  const plainSize = totalSize + nonNullCount * byteArrayPrefixSize
  const dictionaryValueSize = dictSize + dictionary.length * byteArrayPrefixSize
  if (!forced && 2 * (dictionaryValueSize + indexSize) > plainSize) return {}

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
