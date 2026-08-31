declare module "@esri/arcgis-to-geojson-utils" {
  /**
   * Minimal checked contract used by the Broward seed builder.
   *
   * The upstream package does not publish TypeScript declarations. Its
   * converter accepts ArcGIS feature JSON and returns GeoJSON-compatible
   * feature JSON; callers perform their own runtime shape checks.
   */
  const arcgisToGeoJsonUtils: {
    readonly arcgisToGeoJSON: (feature: unknown) => unknown;
  };

  export default arcgisToGeoJsonUtils;
}
