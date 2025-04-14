import fs from 'fs'
import { asyncBufferFromFile, parquetMetadataAsync, parquetReadObjects } from 'hyparquet'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { parquetWriteFile } from '../src/index.js'
import { exampleData, exampleMetadata } from './example.js'

const filedir = 'data/'
const filename = 'data/write.file.parquet'

describe('parquetWriteFile', () => {
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
    parquetWriteFile({ filename, columnData: exampleData })

    // check parquet metadata
    const file = await asyncBufferFromFile(filename)
    const metadata = await parquetMetadataAsync(file)
    expect(metadata).toEqual(exampleMetadata)

    // check parquet data
    const result = await parquetReadObjects({ file, metadata })
    expect(result).toEqual([
      { bool: true, int: 0, bigint: 0n, float: 0, double: 0, string: 'a', nullable: true },
      { bool: false, int: 127, bigint: 127n, float: 0.00009999999747378752, double: 0.0001, string: 'b', nullable: false },
      { bool: true, int: 0x7fff, bigint: 0x7fffn, float: 123.45600128173828, double: 123.456, string: 'c', nullable: null },
      { bool: false, int: 0x7fffffff, bigint: 0x7fffffffffffffffn, float: Infinity, double: 1e100, string: 'd', nullable: null },
    ])
  })
})
