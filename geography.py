"""
This module have utilities to work with latitudes and longitudes
"""

import polars as pl
import json
from shapely.geometry import shape, Point


# %%
def reverse_geocoder(
    df: pl.DataFrame,
    geojson_path: str,
    latitude_name: str,
    longitude_name: str,
    geojson_property: str,
) -> pl.DataFrame:
    """
    Given a geojson file and a dataframe with latitudes and longitudes, this
    function with allow to identify which subshape contains it, and will add some
    property of the geojson

    Args:
        df (pl.DataFrame): Polars dataframe with the latitude and longitude columns
        geojson_path (str): complete path of the geojson file
        latitude_name (str): Name of the latitude column in the dataframe df
        longitude_name (str): Name of the longitude column in the dataframe df
        geojson_property (str): Property you want to add to the final dataframe, like city, state, etc...

    Returns:
        pl.DataFrame: The original df dataframe with a new property column
    """
    with open(geojson_path) as geopmap:
        geojson = json.load(geomap)

    # creating the shapes as a geometry object of shapely
    shapes = []
    properties = []
    for feature in geojson["features"]:
        shapes.append(shape(feature["geometry"]))
        properties.append(feature["properties"][geojson_property])

    # construct point based on lon/lat returned by geocoder
    lat = pl.col(latitude_name)
    lon = pl.col(longitude_name)

    def check_point_in(x):
        """auxiliar function to check if a coordinate is in a shape"""
        for shp, prt in zip(shapes, properties):
            if shp.contains(x):
                return prt
        return "non_founded"

    df_final = (
        df.with_columns(pl.concat_list(lon, lat).alias("coordinate"))
        .with_columns(pl.col("coordinate").apply(lambda x: Point(x)))
        .with_columns(pl.col("coordinate").apply(check_point_in))
    )

    return df_final
