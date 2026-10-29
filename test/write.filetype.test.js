import { parquetMetadata, parquetReadObjects } from 'hyparquet'
import { describe, expect, it } from 'vitest'
import { parquetWriteBuffer } from '../src/index.js'

const empty = { uri: null, offset: null, size: null, content_type: null, checksum: null, inline: null }

describe('parquetWrite FILE logical type', () => {
  it('writes FILE columns', async () => {
    const data = [
      { inline: new Uint8Array([0, 255, 128]), content_type: 'image/png' },
      { inline: new Uint8Array(), uri: 's3://bucket/original', offset: 3n, size: 0n },
      { uri: 's3://bucket/whole', checksum: 'ETAG:abc' },
      { uri: 'relative.bin', offset: 9007199254740993n, size: 7n },
      null,
    ]
    const file = parquetWriteBuffer({ columnData: [{ name: 'file', data, type: 'FILE' }] })

    const metadata = parquetMetadata(file)
    expect(metadata.schema[1]).toEqual({
      name: 'file',
      repetition_type: 'OPTIONAL',
      num_children: 6,
      logical_type: { type: 'FILE' },
    })
    expect(metadata.schema.slice(2).map(e => e.name))
      .toEqual(['uri', 'offset', 'size', 'content_type', 'checksum', 'inline'])

    // inline stays binary even with utf8 conversion
    expect(await parquetReadObjects({ file })).toEqual([
      { file: { ...empty, inline: new Uint8Array([0, 255, 128]), content_type: 'image/png' } },
      { file: { ...empty, inline: new Uint8Array(), uri: 's3://bucket/original', offset: 3n, size: 0n } },
      { file: { ...empty, uri: 's3://bucket/whole', checksum: 'ETAG:abc' } },
      { file: { ...empty, uri: 'relative.bin', offset: 9007199254740993n, size: 7n } },
      { file: undefined },
    ])
  })

  it('writes required FILE columns', async () => {
    const data = [{ uri: 'a.txt' }, { uri: 'b.txt', size: 5n }]
    const file = parquetWriteBuffer({ columnData: [{ name: 'file', data, type: 'FILE', nullable: false }] })
    expect(parquetMetadata(file).schema[1].repetition_type).toBe('REQUIRED')
    expect(await parquetReadObjects({ file })).toEqual([
      { file: { ...empty, uri: 'a.txt' } },
      { file: { ...empty, uri: 'b.txt', size: 5n } },
    ])
  })

  it('writes FILE groups from an explicit schema', async () => {
    const file = parquetWriteBuffer({
      columnData: [{ name: 'image', data: [{ inline: new Uint8Array([1, 2]) }, null] }],
      schema: [
        { name: 'root', num_children: 1 },
        { name: 'image', repetition_type: 'OPTIONAL', num_children: 1, logical_type: { type: 'FILE' } },
        { name: 'inline', type: 'BYTE_ARRAY', repetition_type: 'OPTIONAL' },
      ],
    })
    expect(parquetMetadata(file).schema[1].logical_type).toEqual({ type: 'FILE' })
    expect(await parquetReadObjects({ file })).toEqual([
      { image: { inline: new Uint8Array([1, 2]) } },
      { image: undefined },
    ])
  })
})
