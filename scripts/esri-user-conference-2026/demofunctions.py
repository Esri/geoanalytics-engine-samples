import geoanalytics
import geoanalytics.sql.functions as ST
import geoanalytics.raster.functions as RT
import geoanalytics.tracks.functions as TRK
from geoanalytics.tools import *
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

class GeofenceEvents():

    def __init__(self):
      self.polygons_df = None
      self.mode="long"
    
    def setGeofencePolygons(self, polygons_df):
        self.polygons_df = polygons_df
        return self

    def run(self, tracks_df):
        assert self.polygons_df is not None, "Geofence polygons not set"
        assert tracks_df is not None, "Tracks not set"

        polygons_geom = self.polygons_df[self.polygons_df.st.get_geometry_field()]
        tracks_geom = tracks_df[tracks_df.st.get_geometry_field()]

        return (
          tracks_df.join(self.polygons_df, ST.intersects(tracks_geom, polygons_geom))
            .withColumn("ee_points", F.explode(TRK.entry_exit_points(tracks_geom, polygons_geom)))
            .withColumn("event_duration", F.expr("round((unix_seconds(ee_points.exit.time) - unix_seconds(ee_points.entry.time)) / 3600, 2)"))
            .withColumn("entry_event", F.expr("""
                struct(
                  ee_points.entry.time as event_time,
                  ee_points.entry.point as event_location,
                  case when ee_points.entry.track_endpoint = True then 'Begin' else "Enter" end as event_type
                )
            """))
            .withColumn("exit_event", F.expr("""
                struct(
                  ee_points.exit.time as event_time,
                  ee_points.exit.point as event_location,
                  case when ee_points.exit.track_endpoint = True then 'End' else "Exit" end as event_type
                )
            """))
            .selectExpr("*", "inline(array(entry_event, exit_event))")
            .drop("ee_points", "entry_event", "exit_event", polygons_geom, tracks_geom)
        )


def bbox_polygon(xmin, ymin, xmax, ymax):
    return ST.envelope(ST.multipoint(F.array(ST.point(xmin, ymin), ST.point(xmax, ymax))))


def bbox_clip(geom, xmin, ymin, xmax, ymax):
    return ST.intersection(geom, bbox_polygon(xmin, ymin, xmax, ymax))


def clamp(value_col, min_col, max_col):
    return F.greatest(min_col, F.least(value_col, max_col))


def trk_query_timestamp(track_col, timestamp_col):
    """
    Query for point along track at the specificed timestamp.
    """
    relative_offset_seconds = F.unix_seconds(timestamp_col) - F.unix_seconds(TRK.start_timestamp(track_col))
    offest_clamped = clamp(relative_offset_seconds, F.lit(0), TRK.duration(track_col, "seconds"))
    return TRK.query(track_col, (offest_clamped, "seconds"))


def make_crossing_lines(df, group_field="GROUP_ID"):
    geometry_field = df.st.get_geometry_field()
    agg_exprs = [
        F.count("*").alias("count"),
        ST.simplify(ST.aggr_linestring(geometry_field, F.struct(ST.x(geometry_field), ST.y(geometry_field)))).alias("crossing_line"),
        F.collect_set("port_name").cast("string").alias("ports")
    ]
    return df.groupBy(group_field).agg(*agg_exprs).where("not ST_IsEmpty(crossing_line)")


def as_points(track_df, track_col=None):
    if track_col is None:
        track_col = track_df.st.get_geometry_field()

    return track_df.withColumn(f"{track_col}_point", F.explode(ST.points(track_col)))\
                   .withColumn(f"{track_col}_timestamp", F.timestamp_seconds(ST.m(f"{track_col}_point")))\
                   .drop(track_col)

DataFrame.make_crossing_lines = make_crossing_lines
DataFrame.as_points = as_points



