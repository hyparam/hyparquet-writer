import fs from 'fs'
import { asyncBufferFromFile, parquetMetadataAsync, parquetReadObjects } from 'hyparquet'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { exampleMetadata } from './metadata.test.js'
import { parquetWriteFile } from '../src/index.js'
import { basicData } from './write.buffer.test.js'

const filedir = 'data/'
const filename = 'data/write.file.parquet'

describe('parquetWrite with FileWriter', () => {
  beforeEach(() => {
    // ensure data directory exists
    if (!fs.existsSync(filedir)) {
      fs.mkdirSync(filedir)
    }
  })

  afterEach(() => {
    // remove test file
    if (fs.existsSync(filename)) {
      fs.unlinkSync(filename)
    }
  })

  it('writes parquet file', async () => {
    parquetWriteFile({ filename, columnData: basicData })

    // check parquet metadata
    const file = await asyncBufferFromFile(filename)
    const metadata = await parquetMetadataAsync(file)
    expect(metadata).toEqual(exampleMetadata)

    // check parquet data
    const result = await parquetReadObjects({ file, metadata })
    expect(result).toEqual([
      { bool: true, int: 0, bigint: 0n, double: 0, string: 'a', nullable: true },
      { bool: false, int: 127, bigint: 127n, double: 0.0001, string: 'b', nullable: false },
      { bool: true, int: 0x7fff, bigint: 0x7fffn, double: 123.456, string: 'c', nullable: null },
      { bool: false, int: 0x7fffffff, bigint: 0x7fffffffffffffffn, double: 1e100, string: 'd', nullable: null },
    ])
  })
})
