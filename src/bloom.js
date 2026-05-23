// Split Block Bloom Filter (https://github.com/apache/parquet-format/blob/master/BloomFilter.md)
// A bloom filter is a sequence of 32-byte blocks. Each block holds 8 little-endian uint32 words.
// Insertion sets one bit per word, chosen by salting the low 32 bits of an xxhash64.
// Membership requires all 8 bits to be set; misses are exact, hits are probabilistic.

import { serializeTCompactProtocol } from './thrift.js'

/**
 * @import {Writer} from '../src/types.js'
 */

const SALT = new Uint32Array([
  0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
  0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31,
])

const BYTES_PER_BLOCK = 32
const MIN_BYTES = 32 // one block
const MAX_BYTES = 128 * 1024 * 1024 // parquet-mr default cap

/**
 * Map the high 32 bits of a hash to a block index in [0, numBlocks).
 *
 * @param {bigint} hash
 * @param {number} numBlocks
 * @returns {number}
 */
function blockIndex(hash, numBlocks) {
  return Number((hash >> 32n) * BigInt(numBlocks) >> 32n)
}

/**
 * Per-block mask: 8 uint32 words, each with a single bit set at position `(low32 * SALT[i]) >> 27`.
 *
 * @param {bigint} hash
 * @returns {Uint32Array}
 */
function blockMask(hash) {
  const m = new Uint32Array(8)
  const low = Number(hash & 0xffffffffn) | 0
  for (let i = 0; i < 8; i++) {
    m[i] = 1 << (Math.imul(low, SALT[i]) >>> 27)
  }
  return m
}

/**
 * Insert a hash into a Split Block Bloom Filter.
 *
 * @param {Uint32Array} blocks bloom filter words (8 * numBlocks long)
 * @param {bigint} hash 64-bit xxhash of the parquet-plain-encoded value
 */
export function sbbfInsert(blocks, hash) {
  const offset = blockIndex(hash, blocks.length >> 3) << 3
  const m = blockMask(hash)
  for (let i = 0; i < 8; i++) {
    blocks[offset + i] |= m[i]
  }
}

/**
 * Test whether a hash might be present in a Split Block Bloom Filter.
 * False positives are possible; false negatives are not.
 *
 * @param {Uint32Array} blocks bloom filter words (8 * numBlocks long)
 * @param {bigint} hash 64-bit xxhash of the parquet-plain-encoded value
 * @returns {boolean}
 */
export function sbbfContains(blocks, hash) {
  const offset = blockIndex(hash, blocks.length >> 3) << 3
  const m = blockMask(hash)
  for (let i = 0; i < 8; i++) {
    if ((blocks[offset + i] & m[i]) === 0) return false
  }
  return true
}

/**
 * Round up to the next power of two (32-bit).
 *
 * @param {number} n
 * @returns {number}
 */
function nextPowerOfTwo(n) {
  let p = 1
  while (p < n) p <<= 1
  return p
}

/**
 * Optimal SBBF size in bytes for a given number of distinct values and
 * target false-positive probability. Matches parquet-mr's BlockSplitBloomFilter:
 * derives bits from m = -8 * ndv / ln(1 - p^(1/8)), rounds up to a whole block,
 * and snaps to the next power of two below 1024 bits.
 *
 * @param {number} ndv expected number of distinct values
 * @param {number} fpp target false positive probability, in (0, 1)
 * @returns {number} bloom filter size in bytes (multiple of 32)
 */
export function optimalNumBytes(ndv, fpp) {
  if (!(fpp > 0 && fpp < 1)) throw new Error(`bloom filter fpp must be in (0, 1), got ${fpp}`)
  if (!(ndv >= 0)) throw new Error(`bloom filter ndv must be >= 0, got ${ndv}`)
  const m = -8 * ndv / Math.log(1 - fpp ** (1 / 8))
  let numBits = Math.ceil(m)
  if (!isFinite(numBits) || numBits > MAX_BYTES << 3) numBits = MAX_BYTES << 3
  // Round up to whole 32-byte blocks
  const blockBits = BYTES_PER_BLOCK << 3
  numBits = Math.ceil(numBits / blockBits) * blockBits
  let numBytes = numBits >> 3
  if (numBytes < MIN_BYTES) numBytes = MIN_BYTES
  // Power-of-two snap below 1024 bytes (matches parquet-mr behavior)
  if (numBytes < 1024) numBytes = nextPowerOfTwo(numBytes)
  return numBytes
}

/**
 * Allocate a zeroed Split Block Bloom Filter sized for the given NDV and FPP.
 *
 * @param {number} ndv expected number of distinct values
 * @param {number} [fpp] target false positive probability, default 0.01
 * @returns {Uint32Array} blocks (numBytes / 4 uint32 words)
 */
export function createBloomFilter(ndv, fpp = 0.01) {
  const numBytes = optimalNumBytes(ndv, fpp)
  return new Uint32Array(numBytes >> 2)
}

/**
 * Write a parquet bloom filter: BloomFilterHeader thrift struct followed by
 * the raw little-endian bytes of the SBBF blocks. Always uses BLOCK / XXHASH /
 * UNCOMPRESSED, the only variants parquet currently defines.
 *
 * @param {Writer} writer
 * @param {Uint32Array} blocks bloom filter words (8 * numBlocks long)
 */
export function writeBloomFilter(writer, blocks) {
  if (blocks.length % 8 !== 0) {
    throw new Error(`bloom filter block count must be a multiple of 8 uint32 words, got ${blocks.length}`)
  }
  serializeTCompactProtocol(writer, {
    field_1: blocks.byteLength, // numBytes
    field_2: { field_1: {} }, // algorithm: SplitBlockAlgorithm
    field_3: { field_1: {} }, // hash: XxHash
    field_4: { field_1: {} }, // compression: Uncompressed
  })
  for (let i = 0; i < blocks.length; i++) {
    writer.appendUint32(blocks[i])
  }
}
