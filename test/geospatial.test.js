import { describe, expect, it } from 'vitest'
import { geospatialStatistics } from '../src/geospatial.js'

describe('geospatialStatistics', () => {
  it('computes bounding boxes and geospatial type codes for nested inputs', () => {
    const result = geospatialStatistics([
      null,
      undefined,
      { type: 'Point', coordinates: [1, 2] },
      {
        type: 'LineString',
        coordinates: [
          [5, -1, 10],
          [0, 3, -5],
          [2, 2, undefined],
          [6, 1, Infinity],
        ],
      },
      {
        type: 'Polygon',
        coordinates: [
          [
            [9, 9, 1, 5],
            [9, 10, 3, 5],
            [8, 9, -4, 8],
            [7, 8, Infinity, Infinity],
          ],
        ],
      },
      {
        type: 'MultiPoint',
        coordinates: [
          [-5, -5, 0, -10],
          [4, 4, 12, undefined],
        ],
      },
      { type: 'MultiPolygon', coordinates: [] },
      {
        type: 'MultiLineString',
        coordinates: [
          [
            [
              [Infinity, 0],
            ],
          ],
        ],
      },
      {
        type: 'GeometryCollection',
        geometries: [
          { type: 'Point', coordinates: [2, -3, 7, 9] },
          { type: 'MultiPoint', coordinates: [[60, 10, 0, 11], [3, 6]] },
        ],
      },
      { type: 'GeometryCollection', geometries: [] },
    ])

    expect(result).toEqual({
      bbox: {
        xmin: -5,
        xmax: 60,
        ymin: -5,
        ymax: 10,
        zmin: -5,
        zmax: 12,
        mmin: -10,
        mmax: 11,
      },
      geospatial_types: [1, 5, 6, 7, 1002, 3003, 3004, 3007],
    })
  })

  it('omits geospatial statistics when only null-like values are present', () => {
    const result = geospatialStatistics([null, undefined, null])
    expect(result).toBeUndefined()
  })

  it('tracks type codes even when coordinates are empty', () => {
    const result = geospatialStatistics([
      { type: 'Point', coordinates: [] },
    ])
    expect(result).toEqual({
      bbox: undefined,
      geospatial_types: [1],
    })
  })

  it('throws on invalid value types and geometry definitions', () => {
    expect(() => geospatialStatistics(['oops'])).toThrow('geospatial column expects GeoJSON geometries')
    expect(() => geospatialStatistics([{ type: 'Unknown', coordinates: [] }])).toThrow('unknown geometry type: Unknown')
    expect(() => geospatialStatistics([{ type: 'Point', coordinates: [0, 0, 0, 0, 0] }])).toThrow('unsupported geometry dimensions: 5')
  })
})
