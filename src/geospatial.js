/**
 * Compute geospatial statistics for GEOMETRY and GEOGRAPHY columns.
 *
 * @import {BoundingBox, DecodedArray, Geometry, GeospatialStatistics} from 'hyparquet/src/types.js'
 * @param {DecodedArray} values
 * @returns {GeospatialStatistics | undefined}
 */
export function geospatialStatistics(values) {
  /** @type {Set<number>} */
  const typeCodes = new Set()
  /** @type {BoundingBox | undefined} */
  let bbox

  for (const value of values) {
    if (value === null || value === undefined) continue
    if (typeof value !== 'object') {
      throw new Error('geospatial column expects GeoJSON geometries')
    }
    bbox = extendBoundsFromGeometry(bbox, value)
    typeCodes.add(geometryTypeCodeWithDimension(value))
  }

  if (typeCodes.size || bbox) {
    return {
      bbox,
      // Geospatial type codes of all instances, or an empty list if not known
      geospatial_types: typeCodes.size ? Array.from(typeCodes).sort((a, b) => a - b) : [],
    }
  }
}

/**
 * @param {BoundingBox | undefined} bbox
 * @param {Geometry} geometry
 * @returns {BoundingBox | undefined}
 */
function extendBoundsFromGeometry(bbox, geometry) {
  if (geometry.type === 'GeometryCollection') {
    for (const child of geometry.geometries || []) {
      bbox = extendBoundsFromGeometry(bbox, child)
    }
    return bbox
  }
  return extendBoundsFromCoordinates(bbox, geometry.coordinates)
}

/**
 * @param {BoundingBox | undefined} bbox
 * @param {any[]} coordinates
 * @returns {BoundingBox | undefined}
 */
function extendBoundsFromCoordinates(bbox, coordinates) {
  if (typeof coordinates[0] === 'number') {
    return grow(bbox, coordinates)
  }
  for (const child of coordinates) {
    bbox = extendBoundsFromCoordinates(bbox, child)
  }
  return bbox
}

/**
 * Initialize or expand bbox with a single position [x,y,(z),(m)].
 * @param {BoundingBox | undefined} bbox
 * @param {number[]} position
 * @returns {BoundingBox | undefined}
 */
function grow(bbox, position) {
  const x = position[0]
  const y = position[1]
  if (!Number.isFinite(x) || !Number.isFinite(y)) return bbox

  if (!bbox) {
    bbox = { xmin: x, ymin: y, xmax: x, ymax: y }
  } else {
    updateAxis(bbox, 'xmin', 'xmax', x)
    updateAxis(bbox, 'ymin', 'ymax', y)
  }

  if (position.length > 2) updateAxis(bbox, 'zmin', 'zmax', position[2])
  if (position.length > 3) updateAxis(bbox, 'mmin', 'mmax', position[3])
  return bbox
}

/**
 * @param {BoundingBox} bbox
 * @param {'xmin' | 'ymin' | 'zmin' | 'mmin'} minKey
 * @param {'xmax' | 'ymax' | 'zmax' | 'mmax'} maxKey
 * @param {number | undefined} value
 */
function updateAxis(bbox, minKey, maxKey, value) {
  if (value === undefined || !Number.isFinite(value)) return
  if (bbox[minKey] === undefined || value < bbox[minKey]) bbox[minKey] = value
  if (bbox[maxKey] === undefined || value > bbox[maxKey]) bbox[maxKey] = value
}

/**
 * @param {Geometry} geometry
 * @returns {number}
 */
function geometryTypeCodeWithDimension(geometry) {
  const base = geometryTypeCodes[geometry.type]
  if (base === undefined) throw new Error(`unknown geometry type: ${geometry.type}`)
  const dim = inferGeometryDimensions(geometry)
  if (dim === 2) return base
  if (dim === 3) return base + 1000
  if (dim === 4) return base + 3000
  throw new Error(`unsupported geometry dimensions: ${dim}`)
}

const geometryTypeCodes = {
  Point: 1,
  LineString: 2,
  Polygon: 3,
  MultiPoint: 4,
  MultiLineString: 5,
  MultiPolygon: 6,
  GeometryCollection: 7,
}

/**
 * Determine the maximum coordinate dimensions for the geometry.
 * @param {Geometry} geometry
 * @returns {number}
 */
function inferGeometryDimensions(geometry) {
  if (geometry.type === 'GeometryCollection') {
    let maxDim = 0
    for (const child of geometry.geometries || []) {
      maxDim = Math.max(maxDim, inferGeometryDimensions(child))
    }
    return maxDim || 2
  }
  return inferCoordinateDimensions(geometry.coordinates)
}

/**
 * @param {any[]} value
 * @returns {number}
 */
function inferCoordinateDimensions(value) {
  if (!value.length) return 2
  if (typeof value[0] === 'number') return value.length
  let maxDim = 0
  for (const item of value) {
    maxDim = Math.max(maxDim, inferCoordinateDimensions(item))
  }
  return maxDim || 2
}
