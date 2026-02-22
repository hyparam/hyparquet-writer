/**
 * The MIT License (MIT)
 * Copyright (c) 2016 Zhipeng Jia
 * https://github.com/zhipeng-jia/snappyjs
 */

import { ByteWriter } from './bytewriter.js'

/**
 * @import {Writer} from '../src/types.js'
 */

const BLOCK_LOG = 16
const BLOCK_SIZE = 1 << BLOCK_LOG

const MAX_HASH_TABLE_BITS = 14
const globalHashTables = new Array(MAX_HASH_TABLE_BITS + 1)

/**
 * Compress snappy data.
 * Returns Snappy-compressed bytes as Uint8Array.
 *
 * @param {Uint8Array} input - uncompressed data
 * @returns {Uint8Array}
 */
export function snappyCompress(input) {
  const writer = new ByteWriter()
  writer.appendVarInt(input.length) // uncompressed length

  // Process input in 64K blocks
  let pos = 0
  while (pos < input.length) {
    const fragmentSize = Math.min(input.length - pos, BLOCK_SIZE)
    compressFragment(writer, input, pos, fragmentSize)
    pos += fragmentSize
  }

  return new Uint8Array(writer.getBuffer())
}

/**
 * Hash function used in the reference implementation.
 *
 * @param {number} key
 * @param {number} hashFuncShift
 * @returns {number}
 */
function hashFunc(key, hashFuncShift) {
  return key * 0x1e35a7bd >>> hashFuncShift
}

/**
 * Load a 32-bit little-endian integer from a byte array.
 *
 * @param {Uint8Array} array
 * @param {number} pos
 * @returns {number}
 */
function load32(array, pos) {
  return (
    array[pos] +
    (array[pos + 1] << 8) +
    (array[pos + 2] << 16) +
    (array[pos + 3] << 24)
  )
}

/**
 * Compare two 32-bit sequences for equality.
 *
 * @param {Uint8Array} array
 * @param {number} pos1
 * @param {number} pos2
 * @returns {boolean}
 */
function equals32(array, pos1, pos2) {
  return (
    array[pos1] === array[pos2] &&
    array[pos1 + 1] === array[pos2 + 1] &&
    array[pos1 + 2] === array[pos2 + 2] &&
    array[pos1 + 3] === array[pos2 + 3]
  )
}

/**
 * Emit a literal chunk of data.
 * @param {Writer} writer
 * @param {Uint8Array} input
 * @param {number} ip
 * @param {number} len
 */
function emitLiteral(writer, input, ip, len) {
  // The first byte(s) encode the literal length
  if (len <= 60) {
    writer.appendUint8(len - 1 << 2)
  } else if (len < 256) {
    writer.appendUint8(60 << 2)
    writer.appendUint8(len - 1)
  } else {
    writer.appendUint8(61 << 2)
    writer.appendUint8(len - 1 & 0xff)
    writer.appendUint8(len - 1 >>> 8)
  }

  // Then copy the literal bytes
  writer.appendBytes(input.subarray(ip, ip + len))
}

/**
 * Emit a copy of previous data.
 * @param {Writer} writer
 * @param {number} offset
 * @param {number} len
 */
function emitCopyLessThan64(writer, offset, len) {
  if (len < 12 && offset < 2048) {
    // Copy 4..11 bytes, offset < 2048
    //    --> [  1   | (len-4)<<2 | (offset>>8)<<5 ]
    writer.appendUint8(1 + (len - 4 << 2) + (offset >>> 8 << 5))
    writer.appendUint8(offset & 0xff)
  } else {
    // Copy len bytes, offset 1..65535
    //    --> [  2   | (len-1)<<2 ]
    writer.appendUint8(2 + (len - 1 << 2))
    writer.appendUint8(offset & 0xff)
    writer.appendUint8(offset >>> 8)
  }
}

/**
 * Emit a copy of previous data.
 * @param {Writer} writer
 * @param {number} offset
 * @param {number} len
 */
function emitCopy(writer, offset, len) {
  // Emit 64-byte copies as long as we can
  while (len >= 68) {
    emitCopyLessThan64(writer, offset, 64)
    len -= 64
  }
  // Emit one 60-byte copy if needed
  if (len > 64) {
    emitCopyLessThan64(writer, offset, 60)
    len -= 60
  }
  // Final copy
  emitCopyLessThan64(writer, offset, len)
}

/**
 * Compress a fragment of data.
 * @param {Writer} writer
 * @param {Uint8Array} input
 * @param {number} ip
 * @param {number} inputSize
 */
function compressFragment(writer, input, ip, inputSize) {
  let hashTableBits = 1
  while (1 << hashTableBits <= inputSize && hashTableBits <= MAX_HASH_TABLE_BITS) {
    hashTableBits++
  }
  hashTableBits--
  const hashFuncShift = 32 - hashTableBits

  // Initialize the hash table
  globalHashTables[hashTableBits] ??= new Uint16Array(1 << hashTableBits)
  const hashTable = globalHashTables[hashTableBits]
  hashTable.fill(0)

  const ipEnd = ip + inputSize
  let ipLimit
  const baseIp = ip
  let nextEmit = ip

  let hash, nextHash
  let nextIp, candidate, skip
  let bytesBetweenHashLookups
  let base, matched, offset
  let prevHash, curHash
  let flag = true

  const INPUT_MARGIN = 15
  if (inputSize >= INPUT_MARGIN) {
    ipLimit = ipEnd - INPUT_MARGIN
    ip++
    nextHash = hashFunc(load32(input, ip), hashFuncShift)

    while (flag) {
      skip = 32
      nextIp = ip
      do {
        ip = nextIp
        hash = nextHash
        bytesBetweenHashLookups = skip >>> 5
        skip++
        nextIp = ip + bytesBetweenHashLookups
        if (ip > ipLimit) {
          flag = false
          break
        }
        nextHash = hashFunc(load32(input, nextIp), hashFuncShift)
        candidate = baseIp + hashTable[hash]
        hashTable[hash] = ip - baseIp
      } while (!equals32(input, ip, candidate))

      if (!flag) {
        break
      }

      // Emit the literal from `nextEmit` to `ip`
      emitLiteral(writer, input, nextEmit, ip - nextEmit)

      // We found a match. Repeatedly match and emit copies
      do {
        base = ip
        matched = 4
        while (
          ip + matched < ipEnd &&
          input[ip + matched] === input[candidate + matched]
        ) {
          matched++
        }
        ip += matched
        offset = base - candidate
        emitCopy(writer, offset, matched)

        nextEmit = ip
        if (ip >= ipLimit) {
          flag = false
          break
        }
        prevHash = hashFunc(load32(input, ip - 1), hashFuncShift)
        hashTable[prevHash] = ip - 1 - baseIp
        curHash = hashFunc(load32(input, ip), hashFuncShift)
        candidate = baseIp + hashTable[curHash]
        hashTable[curHash] = ip - baseIp
      } while (equals32(input, ip, candidate))

      if (!flag) {
        break
      }

      ip++
      nextHash = hashFunc(load32(input, ip), hashFuncShift)
    }
  }

  // Emit the last literal (if any)
  if (nextEmit < ipEnd) {
    emitLiteral(writer, input, nextEmit, ipEnd - nextEmit)
  }
}
