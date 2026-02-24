/**
 * @import {DecodedArray} from 'hyparquet'
 * @import {Writer} from '../src/types.js'
 */

/**
 * @param {Writer} writer
 * @param {DecodedArray} values
 * @param {number} bitWidth
 * @returns {number} bytes written
 */
export function writeRleBitPackedHybrid(writer, values, bitWidth) {
  const offsetStart = writer.offset
  let pendingBitPackedGroups = 0
  let bitPackedStart = 0
  let i = 0

  while (i < values.length) {
    // Try to write RLE runs of 8+ values
    let rleCount = 1
    const firstVal = values[i]
    while (i + rleCount < values.length && values[i + rleCount] === firstVal) {
      rleCount++
    }
    if (rleCount >= 8) {
      // Flush pending bit-packed groups
      if (pendingBitPackedGroups) {
        writeBitPackedGroups(writer, values, bitPackedStart, pendingBitPackedGroups, bitWidth)
        pendingBitPackedGroups = 0
      }

      // Write RLE run
      writeRleRun(writer, firstVal, rleCount, bitWidth)
      i += rleCount
    } else {
      // Add to pending bit-packed groups
      if (pendingBitPackedGroups === 0) {
        bitPackedStart = i
      }
      pendingBitPackedGroups++
      i += 8
    }
  }

  // Flush remaining
  if (pendingBitPackedGroups) {
    writeBitPackedGroups(writer, values, bitPackedStart, pendingBitPackedGroups, bitWidth)
  }

  return writer.offset - offsetStart
}

/**
 * Write a single RLE run: a repeated value and its count.
 *
 * @param {Writer} writer
 * @param {number} value
 * @param {number} count
 * @param {number} bitWidth
 */
function writeRleRun(writer, value, count, bitWidth) {
  writer.appendVarInt(count << 1) // rle header
  const width = bitWidth + 7 >> 3
  for (let j = 0; j < width; j++) {
    writer.appendUint8(value >> (j << 3) & 0xff)
  }
}

/**
 * Write consecutive bit-packed groups of 8 values each.
 *
 * @param {Writer} writer
 * @param {DecodedArray} values
 * @param {number} start index of first value
 * @param {number} numGroups number of 8-value groups
 * @param {number} bitWidth
 */
function writeBitPackedGroups(writer, values, start, numGroups, bitWidth) {
  writer.appendVarInt(numGroups << 1 | 1) // bp header

  if (bitWidth === 0) return

  const mask = (1 << bitWidth) - 1
  let buffer = 0
  let bitsUsed = 0
  const totalValues = numGroups * 8

  for (let i = 0; i < totalValues; i++) {
    const idx = start + i
    const v = idx < values.length ? values[idx] & mask : 0
    buffer |= v << bitsUsed
    bitsUsed += bitWidth

    // Flush full bytes
    while (bitsUsed >= 8) {
      writer.appendUint8(buffer & 0xff)
      buffer >>>= 8
      bitsUsed -= 8
    }
  }

  // Flush any remaining bits
  if (bitsUsed > 0) {
    writer.appendUint8(buffer & 0xff)
  }
}
