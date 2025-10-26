import { parquetMetadata } from 'hyparquet'
import { describe, expect, it } from 'vitest'
import { parquetWriteBuffer } from '../src/index.js'

/**
 * @import {ColumnSource} from '../src/types.js'
 */

describe('geospatial statistics', () => {
  it('writes geospatial statistics into column metadata', () => {
    /** @type {ColumnSource[]} */
    const columnData = [{
      name: 'geometry',
      type: 'GEOMETRY',
      data: [
        { type: 'Point', coordinates: [10, 5, 100, 2] },
        null,
        {
          type: 'LineString',
          coordinates: [
            [-20, -10, 50, 5],
            [40, 30, 75, -5],
          ],
        },
        {
          type: 'GeometryCollection',
          geometries: [
            { type: 'Point', coordinates: [5, 15] },
            {
              type: 'MultiPoint',
              coordinates: [
                [0, -5],
                [60, 10],
              ],
            },
          ],
        },
      ],
    }]

    const buffer = parquetWriteBuffer({ columnData })
    const metadata = parquetMetadata(buffer)
    const columnMeta = metadata.row_groups[0].columns[0].meta_data

    expect(columnMeta?.statistics).toEqual({ null_count: 1n })
    expect(columnMeta?.geospatial_statistics).toEqual({
      bbox: {
        xmin: -20,
        xmax: 60,
        ymin: -10,
        ymax: 30,
        zmin: 50,
        zmax: 100,
        mmin: -5,
        mmax: 5,
      },
      // sort numerically not by string order
      geospatial_types: [7, 3001, 3002],
    })
  })
})
