import { ByteWriter } from './bytewriter.js'

/**
 * @import {DecodedArray} from 'hyparquet'
 * @import {Writer} from '../src/types.js'
 * @param {Writer} writer
 * @param {DecodedArray} values
 * @returns {number} bytes written
 */
export function writeRleBitPackedHybrid(writer, values) {
  const offsetStart = writer.offset
  // find max bitwidth
  const bitWidth = Math.ceil(Math.log2(Math.max(...values) + 1))

  // try both RLE and bit-packed and choose the best
  const rle = new ByteWriter()
  writeRle(rle, values, bitWidth)
  const bitPacked = new ByteWriter()
  writeBitPacked(bitPacked, values, bitWidth)

  if (rle.offset < bitPacked.offset) {
    writer.appendBuffer(rle.getBuffer())
  } else {
    writer.appendBuffer(bitPacked.getBuffer())
  }

  return writer.offset - offsetStart
}

/**
 * @param {Writer} writer
 * @param {DecodedArray} values
 * @param {number} bitWidth
 */
function writeBitPacked(writer, values, bitWidth) {
  // Number of 8-value groups
  const numGroups = Math.ceil(values.length / 8)

  // The parquet bitpack header: lower bit = 1 => "bit-packed mode"
  // upper bits = number of groups
  const header = numGroups << 1 | 1

  // Write the header as a varint
  writer.appendVarInt(header)

  // If bitWidth = 0, no data is actually needed
  if (bitWidth === 0 || values.length === 0) {
    return
  }

  const mask = (1 << bitWidth) - 1
  let buffer = 0 // accumulates bits
  let bitsUsed = 0 // how many bits are in 'buffer' so far

  // Write out each value, bit-packing into buffer
  for (let i = 0; i < values.length; i++) {
    const v = values[i] & mask // mask off bits exceeding bitWidth
    buffer |= v << bitsUsed
    bitsUsed += bitWidth

    // Flush full bytes
    while (bitsUsed >= 8) {
      writer.appendUint8(buffer & 0xFF)
      buffer >>>= 8
      bitsUsed -= 8
    }
  }

  // Pad the final partial group with zeros if needed
  const totalNeeded = numGroups * 8
  for (let padCount = values.length; padCount < totalNeeded; padCount++) {
    // Just write a 0 value into the buffer
    buffer |= 0 << bitsUsed
    bitsUsed += bitWidth
    while (bitsUsed >= 8) {
      writer.appendUint8(buffer & 0xFF)
      buffer >>>= 8
      bitsUsed -= 8
    }
  }

  // Flush any remaining bits
  if (bitsUsed > 0) {
    writer.appendUint8(buffer & 0xff)
  }
}

/**
 * Run-length encoding: write repeated values by encoding the value and its count.
 *
 * @param {Writer} writer
 * @param {DecodedArray} values
 * @param {number} bitWidth
 */
function writeRle(writer, values, bitWidth) {
  if (!values.length) return

  let currentValue = values[0]
  let count = 1

  for (let i = 1; i <= values.length; i++) {
    if (i < values.length && values[i] === currentValue) {
      count++ // continue the run
    } else {
      // write the count of repeated values
      const header = count << 1
      writer.appendVarInt(header)

      // write the value
      const width = bitWidth + 7 >> 3 // bytes needed
      for (let j = 0; j < width; j++) {
        writer.appendUint8(currentValue >> (j << 3) & 0xff)
      }

      // reset for the next run
      if (i < values.length) {
        currentValue = values[i]
        count = 1
      }
    }
  }
}
