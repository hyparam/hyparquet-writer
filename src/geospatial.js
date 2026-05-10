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
  /** @type {Partial<BoundingBox> | undefined} */
  let partial

  for (const value of values) {
    if (value === null || value === undefined) continue
    if (typeof value !== 'object') {
      throw new Error('geospatial column expects GeoJSON geometries')
    }
    partial = extendBoundsFromGeometry(partial, value)
    typeCodes.add(geometryTypeCodeWithDimension(value))
  }

  // If either the X or Y dimension has no finite values, the bounding box itself is not produced
  /** @type {BoundingBox | undefined} */
  let bbox
  const { xmin, ymin, xmax, ymax } = partial ?? {}
  if (xmin !== undefined && ymin !== undefined && xmax !== undefined && ymax !== undefined) {
    bbox = { ...partial, xmin, ymin, xmax, ymax }
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
 * @param {Partial<BoundingBox> | undefined} bbox
 * @param {Geometry} geometry
 * @returns {Partial<BoundingBox> | undefined}
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
 * Recurse through nested coordinate arrays. At a leaf position [x,y,(z),(m)],
 * each dimension is filtered independently. NaN/non-finite values in one
 * dimension does not skip the others.
 * @param {Partial<BoundingBox> | undefined} bbox
 * @param {any[]} coordinates
 * @returns {Partial<BoundingBox> | undefined}
 */
function extendBoundsFromCoordinates(bbox, coordinates) {
  if (typeof coordinates[0] === 'number') {
    // Expand bbox
    bbox = updateAxis(bbox, 'xmin', 'xmax', coordinates[0])
    bbox = updateAxis(bbox, 'ymin', 'ymax', coordinates[1])
    if (coordinates.length > 2) bbox = updateAxis(bbox, 'zmin', 'zmax', coordinates[2])
    if (coordinates.length > 3) bbox = updateAxis(bbox, 'mmin', 'mmax', coordinates[3])
    return bbox
  }
  for (const child of coordinates) {
    bbox = extendBoundsFromCoordinates(bbox, child)
  }
  return bbox
}

/**
 * @param {Partial<BoundingBox> | undefined} bbox
 * @param {'xmin' | 'ymin' | 'zmin' | 'mmin'} minKey
 * @param {'xmax' | 'ymax' | 'zmax' | 'mmax'} maxKey
 * @param {number | undefined} value
 * @returns {Partial<BoundingBox> | undefined}
 */
function updateAxis(bbox, minKey, maxKey, value) {
  if (value === undefined || !Number.isFinite(value)) return bbox
  if (!bbox) bbox = {}
  const min = bbox[minKey]
  const max = bbox[maxKey]
  if (min === undefined || value < min) bbox[minKey] = value
  if (max === undefined || value > max) bbox[maxKey] = value
  return bbox
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
