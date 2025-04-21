import fs from 'fs'
import { asyncBufferFromFile, parquetMetadataAsync, parquetReadObjects, parquetSchema } from 'hyparquet'
import { describe, expect, it } from 'vitest'
import { parquetWriteBuffer } from '../src/index.js'

describe('parquetWrite round-trip', () => {
  const files = fs.readdirSync('test/files').filter(f => f.endsWith('.parquet'))

  files.forEach(filename => {
    it(`round-trips data from ${filename}`, async () => {
      const file = await asyncBufferFromFile(`test/files/${filename}`)
      const metadata = await parquetMetadataAsync(file)
      const rows = await parquetReadObjects({ file })

      // transpose the row data
      const schema = parquetSchema(metadata)
      const columnData = schema.children.map(({ element }) => ({
        ...element,
        data: /** @type {any[]} */ ([]),
      }))
      for (const row of rows) {
        for (const { name, data } of columnData) {
          data.push(row[name])
        }
      }

      const buffer = parquetWriteBuffer({ columnData })
      const output = await parquetReadObjects({ file: buffer })

      expect(output.length).toBe(rows.length)
      expect(output).toEqual(rows)
    })
  })
})
