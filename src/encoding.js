
/**
 * @import {Writer} from './writer.js'
 * @param {Writer} writer
 * @param {number[]} values
 */
export function writeRleBitPackedHybrid(writer, values) {
  // find max bitwidth
  const bitWidth = Math.ceil(Math.log2(Math.max(...values) + 1))

  // TODO: Try both RLE and bit-packed and choose the best
  writeBitPacked(writer, values, bitWidth)
}

/**
 * @param {Writer} writer
 * @param {number[]} values
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
    writer.appendUint8(buffer & 0xFF)
  }
}
