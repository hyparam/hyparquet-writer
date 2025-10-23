import { describe, expect, it } from 'vitest'
import { geojsonToWkb } from '../src/wkb.js'
import { wkbToGeojson } from 'hyparquet/src/wkb.js'

/** @import {Geometry} from 'hyparquet/src/types.js' */

describe('geojsonToWkb', () => {
  it('encodes point geometries', () => {
    /** @type {Geometry} */
    const geometry = { type: 'Point', coordinates: [30, 10] }
    const decoded = decode(geojsonToWkb(geometry))
    expect(decoded).toEqual(geometry)
  })

  it('encodes polygons with holes', () => {
    /** @type {Geometry} */
    const geometry = {
      type: 'Polygon',
      coordinates: [
        [[35, 10], [45, 45], [15, 40], [10, 20], [35, 10]],
        [[20, 30], [35, 35], [30, 20], [20, 30]],
      ],
    }
    const decoded = decode(geojsonToWkb(geometry))
    expect(decoded).toEqual(geometry)
  })

  it('encodes geometry collections with mixed dimensions', () => {
    /** @type {Geometry} */
    const geometry = {
      type: 'GeometryCollection',
      geometries: [
        { type: 'Point', coordinates: [30, 10, 5] },
        { type: 'LineString', coordinates: [[30, 10, 5], [40, 40, 5], [20, 40, 5], [10, 20, 5]] },
      ],
    }
    const decoded = decode(geojsonToWkb(geometry))
    expect(decoded).toEqual(geometry)
  })
})

/**
 * Decode WKB using the hyparquet reader for verification.
 *
 * @param {Uint8Array} wkb
 * @returns {Geometry}
 */
function decode(wkb) {
  const view = new DataView(wkb.buffer, wkb.byteOffset, wkb.byteLength)
  const reader = { view, offset: 0 }
  return wkbToGeojson(reader)
}
