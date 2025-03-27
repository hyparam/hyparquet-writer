import { describe, expect, it } from 'vitest'
import { snappyCompress } from '../src/snappy.js'
import { Writer } from '../src/writer.js'
import { snappyUncompress } from 'hyparquet'

describe('snappy compress', () => {

  it.for([
    { compressed: [0x00], uncompressed: '' },
    { compressed: [0x01, 0x00, 0x68], uncompressed: 'h' },
    { compressed: [0x02, 0x04, 0x68, 0x79], uncompressed: 'hy' },
    { compressed: [0x03, 0x08, 0x68, 0x79, 0x70], uncompressed: 'hyp' },
    { compressed: [0x05, 0x10, 0x68, 0x79, 0x70, 0x65, 0x72], uncompressed: 'hyper' },
    {
      compressed: [0x0a, 0x24, 0x68, 0x79, 0x70, 0x65, 0x72, 0x70, 0x61, 0x72, 0x61, 0x6d],
      uncompressed: 'hyperparam',
    },
    {
      compressed: [0x15, 0x08, 0x68, 0x79, 0x70, 0x46, 0x03, 0x00],
      uncompressed: 'hyphyphyphyphyphyphyp',
    },
    {
      // from rowgroups.parquet
      compressed: [
        80, 4, 1, 0, 9, 1, 0, 2, 9, 7, 4, 0, 3, 13, 8, 0, 4, 13, 8, 0, 5, 13,
        8, 0, 6, 13, 8, 0, 7, 13, 8, 0, 8, 13, 8, 60, 9, 0, 0, 0, 0, 0, 0, 0,
        10, 0, 0, 0, 0, 0, 0, 0,
      ],
      uncompressed: new Uint8Array([
        1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0,
        0, 4, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0,
        0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0,
        0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0,
      ]),
    },
    // from datapage_v2.snappy.parquet
    { compressed: [2, 4, 0, 3], uncompressed: new Uint8Array([0, 3]) },
    { compressed: [ 6, 20, 2, 0, 0, 0, 3, 23], uncompressed: new Uint8Array([2, 0, 0, 0, 3, 23]) },
    // from sample data test
    {
      compressed: [1, 0, 5],
      uncompressed: new Uint8Array([5]),
    },
  ])('compresses valid input %p', ({ compressed, uncompressed }) => {
    const writer = new Writer()
    const encoder = new TextEncoder()
    const input = typeof uncompressed === 'string' ? encoder.encode(uncompressed) : new Uint8Array(uncompressed)
    snappyCompress(writer, input)
    const output = writer.getBuffer()
    expect(output).toEqual(new Uint8Array(compressed).buffer)
    // re-decompress to verify
    const decompressed = new Uint8Array(input.length)
    snappyUncompress(new Uint8Array(output), decompressed)
    expect(decompressed).toEqual(input)
  })
})
