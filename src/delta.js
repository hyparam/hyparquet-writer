/**
 * Delta Binary Packed encoding for parquet.
 * Encodes integers as deltas with variable bit-width packing.
 *
 * @import {DecodedArray} from 'hyparquet'
 * @import {Writer} from '../src/types.js'
 */

const BLOCK_SIZE = 128
const MINIBLOCKS_PER_BLOCK = 4
const VALUES_PER_MINIBLOCK = BLOCK_SIZE / MINIBLOCKS_PER_BLOCK // 32

/**
 * Write values using delta binary packed encoding.
 *
 * @param {Writer} writer
 * @param {DecodedArray} values
 */
export function deltaBinaryPack(writer, values) {
  const count = values.length
  if (count === 0) {
    // Write header with zero count
    writer.appendVarInt(BLOCK_SIZE)
    writer.appendVarInt(MINIBLOCKS_PER_BLOCK)
    writer.appendVarInt(0)
    writer.appendVarInt(0)
    return
  }
  if (typeof values[0] !== 'number' && typeof values[0] !== 'bigint') {
    throw new Error('deltaBinaryPack only supports number or bigint arrays')
  }

  if (usesInt32NumberPath(values)) {
    deltaBinaryPackInt32(writer, values)
  } else {
    deltaBinaryPackBigInt(writer, values)
  }
}

/**
 * Return whether all values can use exact Number arithmetic. INT32 deltas can
 * span 33 signed bits, but they and their adjusted deltas remain exactly
 * representable as JavaScript numbers.
 *
 * @param {DecodedArray} values
 * @returns {boolean}
 */
function usesInt32NumberPath(values) {
  if (values instanceof Int32Array) return true
  for (let i = 0; i < values.length; i++) {
    const value = values[i]
    if (
      typeof value !== 'number' ||
      !Number.isInteger(value) ||
      value < -0x80000000 ||
      value > 0x7fffffff
    ) return false
  }
  return true
}

/**
 * Write signed 32-bit values without allocating or operating on BigInts.
 *
 * @param {Writer} writer
 * @param {DecodedArray} values
 */
function deltaBinaryPackInt32(writer, values) {
  const count = values.length

  // Write header
  writer.appendVarInt(BLOCK_SIZE)
  writer.appendVarInt(MINIBLOCKS_PER_BLOCK)
  writer.appendVarInt(count)
  appendZigZagNumber(writer, Number(values[0]))

  // Process blocks
  /** @type {Array<Uint8Array | undefined>} */
  const packedByBitWidth = []
  const blockDeltas = new Float64Array(BLOCK_SIZE)
  const bitWidths = new Uint8Array(MINIBLOCKS_PER_BLOCK)
  let index = 1
  while (index < count) {
    const blockEnd = Math.min(index + BLOCK_SIZE, count)
    const blockSize = blockEnd - index

    // Compute deltas for this block
    // Adjacent INT32 extremes differ by 4,294,967,295, so Int32Array is not
    // sufficient even though every input value is a signed 32-bit integer.
    let minDelta = Number(values[index]) - Number(values[index - 1])
    blockDeltas[0] = minDelta
    for (let i = 1; i < blockSize; i++) {
      const delta = Number(values[index + i]) - Number(values[index + i - 1])
      blockDeltas[i] = delta
      if (delta < minDelta) minDelta = delta
    }
    appendZigZagNumber(writer, minDelta)

    // Calculate bit widths for each miniblock
    for (let mb = 0; mb < MINIBLOCKS_PER_BLOCK; mb++) {
      const mbStart = mb * VALUES_PER_MINIBLOCK
      const mbEnd = Math.min(mbStart + VALUES_PER_MINIBLOCK, blockSize)

      let maxAdjusted = 0
      for (let i = mbStart; i < mbEnd; i++) {
        const adjusted = blockDeltas[i] - minDelta
        if (adjusted > maxAdjusted) maxAdjusted = adjusted
      }
      bitWidths[mb] = bitWidthNumber(maxAdjusted)
    }

    // Write bit widths
    writer.appendBytes(bitWidths)

    // Write packed miniblocks
    for (let mb = 0; mb < MINIBLOCKS_PER_BLOCK; mb++) {
      const bitWidth = bitWidths[mb]
      if (bitWidth === 0) continue // No data needed for zero bit width

      const mbStart = mb * VALUES_PER_MINIBLOCK
      const mbEnd = Math.min(mbStart + VALUES_PER_MINIBLOCK, blockSize)

      // Bit pack into a reusable miniblock buffer, then append it in one call.
      // There are always 32 values including padding, so the byte length is
      // exactly bitWidth * 4.
      const packed = packedByBitWidth[bitWidth] ??= new Uint8Array(bitWidth * 4)
      let byteIndex = 0
      let buffer = 0
      let bitsUsed = 0
      if (bitWidth <= 25) {
        // Up to 25 data bits plus at most 7 pending bits fit in the 32-bit
        // bitwise operations, which are faster than arithmetic packing.
        for (let i = 0; i < VALUES_PER_MINIBLOCK; i++) {
          const adjusted = mbStart + i < mbEnd ? blockDeltas[mbStart + i] - minDelta : 0
          buffer |= adjusted << bitsUsed
          bitsUsed += bitWidth
          while (bitsUsed >= 8) {
            packed[byteIndex++] = buffer & 0xff
            buffer >>>= 8
            bitsUsed -= 8
          }
        }
      } else {
        for (let i = 0; i < VALUES_PER_MINIBLOCK; i++) {
          const adjusted = mbStart + i < mbEnd ? blockDeltas[mbStart + i] - minDelta : 0
          // bitsUsed is below 8 at the start of each iteration. The largest
          // adjusted INT32 delta is below 2^33, so this stays below 2^40 and is
          // exactly representable by Number.
          buffer += adjusted * 2 ** bitsUsed
          bitsUsed += bitWidth
          while (bitsUsed >= 8) {
            packed[byteIndex++] = buffer % 0x100
            buffer = Math.floor(buffer / 0x100)
            bitsUsed -= 8
          }
        }
      }
      writer.appendBytes(packed)
      // assert(bitsUsed === 0) // because multiple of 8
    }

    index = blockEnd
  }
}

/**
 * BigInt fallback for INT64 values and numeric input outside the INT32 range.
 *
 * @param {Writer} writer
 * @param {DecodedArray} values
 */
function deltaBinaryPackBigInt(writer, values) {
  const count = values.length

  // Write header
  writer.appendVarInt(BLOCK_SIZE)
  writer.appendVarInt(MINIBLOCKS_PER_BLOCK)
  writer.appendVarInt(count)
  writer.appendZigZag(values[0])

  // Process blocks
  let index = 1
  while (index < count) {
    const blockEnd = Math.min(index + BLOCK_SIZE, count)
    const blockSize = blockEnd - index

    // Compute deltas for this block
    const blockDeltas = new BigInt64Array(blockSize)
    let minDelta = BigInt(values[index]) - BigInt(values[index - 1])
    blockDeltas[0] = minDelta
    for (let i = 1; i < blockSize; i++) {
      const delta = BigInt(values[index + i]) - BigInt(values[index + i - 1])
      blockDeltas[i] = delta
      if (delta < minDelta) minDelta = delta
    }
    writer.appendZigZag(minDelta)

    // Calculate bit widths for each miniblock
    const bitWidths = new Uint8Array(MINIBLOCKS_PER_BLOCK)
    for (let mb = 0; mb < MINIBLOCKS_PER_BLOCK; mb++) {
      const mbStart = mb * VALUES_PER_MINIBLOCK
      const mbEnd = Math.min(mbStart + VALUES_PER_MINIBLOCK, blockSize)

      let maxAdjusted = 0n
      for (let i = mbStart; i < mbEnd; i++) {
        const adjusted = blockDeltas[i] - minDelta
        if (adjusted > maxAdjusted) maxAdjusted = adjusted
      }
      bitWidths[mb] = bitWidth(maxAdjusted)
    }

    // Write bit widths
    writer.appendBytes(bitWidths)

    // Write packed miniblocks
    for (let mb = 0; mb < MINIBLOCKS_PER_BLOCK; mb++) {
      const bitWidth = bitWidths[mb]
      if (bitWidth === 0) continue // No data needed for zero bit width

      const mbStart = mb * VALUES_PER_MINIBLOCK
      const mbEnd = Math.min(mbStart + VALUES_PER_MINIBLOCK, blockSize)

      // Bit pack the adjusted deltas
      let buffer = 0n
      let bitsUsed = 0

      for (let i = 0; i < VALUES_PER_MINIBLOCK; i++) {
        const adjusted = mbStart + i < mbEnd ? blockDeltas[mbStart + i] - minDelta : 0n
        buffer |= adjusted << BigInt(bitsUsed)
        bitsUsed += bitWidth

        // Flush complete bytes
        while (bitsUsed >= 8) {
          writer.appendUint8(Number(buffer & 0xffn))
          buffer >>= 8n
          bitsUsed -= 8
        }
      }
      // assert(bitsUsed === 0) // because multiple of 8
    }

    index = blockEnd
  }
}

/**
 * Zigzag-encode an exactly represented integer and write it as a varint without
 * truncating through JavaScript's 32-bit bitwise operators.
 *
 * @param {Writer} writer
 * @param {number} value
 */
function appendZigZagNumber(writer, value) {
  let encoded = value < 0 ? -value * 2 - 1 : value * 2
  while (encoded >= 0x80) {
    writer.appendUint8(encoded % 0x80 + 0x80)
    encoded = Math.floor(encoded / 0x80)
  }
  writer.appendUint8(encoded)
}

/**
 * Minimum bits needed to store an adjusted INT32 delta.
 *
 * @param {number} value
 * @returns {number}
 */
function bitWidthNumber(value) {
  if (value === 0) return 0
  return value > 0xffffffff ? 33 : 32 - Math.clz32(value)
}

/**
 * Write byte arrays using delta length encoding.
 * Encodes lengths using delta binary packed, then writes raw bytes.
 *
 * @param {Writer} writer
 * @param {DecodedArray} values
 */
export function deltaLengthByteArray(writer, values) {
  // Extract lengths
  const lengths = new Int32Array(values.length)
  for (let i = 0; i < values.length; i++) {
    const value = values[i]
    if (!(value instanceof Uint8Array)) {
      throw new Error('deltaLengthByteArray expects Uint8Array values')
    }
    lengths[i] = value.length
  }

  // Write delta-packed lengths
  deltaBinaryPack(writer, lengths)

  // Write raw byte data
  for (const value of values) {
    writer.appendBytes(value)
  }
}

/**
 * Write byte arrays using delta encoding with prefix compression.
 * Stores common prefixes with previous value to improve compression.
 *
 * @param {Writer} writer
 * @param {DecodedArray} values
 */
export function deltaByteArray(writer, values) {
  if (values.length === 0) {
    deltaBinaryPack(writer, [])
    deltaBinaryPack(writer, [])
    return
  }

  // Calculate prefix lengths and suffixes
  const prefixLengths = new Int32Array(values.length)
  const suffixLengths = new Int32Array(values.length)
  /** @type {Uint8Array[]} */
  const suffixes = new Array(values.length)

  // First value has no prefix
  const value = values[0]
  if (!(value instanceof Uint8Array)) {
    throw new Error('deltaByteArray expects Uint8Array values')
  }
  prefixLengths[0] = 0
  suffixLengths[0] = values[0].length
  suffixes[0] = values[0]

  for (let i = 1; i < values.length; i++) {
    const prev = values[i - 1]
    const curr = values[i]
    if (!(curr instanceof Uint8Array)) {
      throw new Error('deltaByteArray expects Uint8Array values')
    }

    // Find common prefix length
    let prefixLen = 0
    const maxPrefix = Math.min(prev.length, curr.length)
    while (prefixLen < maxPrefix && prev[prefixLen] === curr[prefixLen]) {
      prefixLen++
    }

    prefixLengths[i] = prefixLen
    suffixLengths[i] = curr.length - prefixLen
    suffixes[i] = curr.subarray(prefixLen)
  }

  // Write delta-packed prefix lengths
  deltaBinaryPack(writer, prefixLengths)

  // Write delta-packed suffix lengths
  deltaBinaryPack(writer, suffixLengths)

  // Write suffix bytes
  for (const suffix of suffixes) {
    writer.appendBytes(suffix)
  }
}

/**
 * Minimum bits needed to store value.
 *
 * @param {bigint} value
 * @returns {number}
 */
function bitWidth(value) {
  if (value === 0n) return 0
  let bits = 0
  while (value > 0n) {
    bits++
    value >>= 1n
  }
  return bits
}
