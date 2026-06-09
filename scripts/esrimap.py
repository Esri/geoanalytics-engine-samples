# Disclaimers: 
# * This script is provided as a sample as-is 
# * This script uses the ArcGIS Maps SDK for Javascript. Please refer to the specific
#   terms of use for the SDK: https://developers.arcgis.com/javascript/latest/licensing/

from json import dumps
from string import Template
import math
from IPython.display import display, HTML
import uuid
import base64
import geoanalytics.sql.functions as ST
import geoanalytics.raster.functions as RT
from geoanalytics.sql import PointUDT, MultiPointUDT, LinestringUDT, PolygonUDT, SpatialReference
import pyspark.sql.functions as F

# Disclaimers: 
# * This script is provided as a sample as-is 
# * This script uses the ArcGIS Maps SDK for Javascript. Please refer to the specific
#   terms of use for the SDK: https://developers.arcgis.com/javascript/latest/licensing/

from json import dumps
from string import Template
import math
from IPython.display import display, HTML, Markdown
import uuid
import base64
import geoanalytics.sql.functions as ST
import geoanalytics.raster.functions as RT
from geoanalytics.sql import PointUDT, MultiPointUDT, LinestringUDT, PolygonUDT, SpatialReference
import pyspark.sql.functions as F

class EsriJSMap:
    """
    Simple map display for Python notebooks that uses the ArcGIS Maps SDK for Javascript. 

    Overview
    --------

    IPython.display is used to display HTML which contains all of the HTML, Javascript, and feature data needed 
    to render the map. The feature data is collected from Spark dataframes and converted to a `FeatureLayer` object
    for the Maps SDK. 

    Map views are monitored by a timer thread which checks to see if the map has been removed from the DOM (i.e., the
    notebook cell output has been cleared) and destroys all resources used by the map.

    Warnings:
    ---------

    * Adding too many maps to the notebook can result in undesirable/undefined behavior
    * Adding too many features to a map may cause your browser to become unresponsive or crash
    * The Maps SDK runs within the browser and so any issues relating to map rendering
      will show up in the browser developer console and not in the notebook output
    
    """

    # Maps Spark geometry type to the Javascript class (and type name with .lower())
    _geometry_mapping = {
        PointUDT: "Point",
        PolygonUDT: "Polygon",
        LinestringUDT: "Polyline",
        MultiPointUDT: "Multipoint"
    }

    # Maps the Spark data type string to Esri Maps SDK type string
    _type_mapping = {
        "string": "string",
        "short": "small-integer",
        "integer": "integer",
        "long": "big-integer",
        "float": "single",
        "double": "double"
    }

    @staticmethod
    def display_layer(self, *, basemap="gray-vector", basemap_sr=4326, width="100%", height="800px", fields=None, renderer=None, label=None, popup=None, max_records=100000, **render_args):
        map = EsriJSMap(basemap=basemap, basemap_sr=basemap_sr, width=width, height=height)
        map.add_layer(self, renderer=renderer, label=label, popup=popup, max_records=max_records, fields=fields, **render_args)
        map.display()

    @staticmethod
    def display_raster_layer(self, *, raster_field="raster", basemap="gray-vector", basemap_sr=4326, width="100%", height="800px", ):
        map = EsriJSMap(basemap=basemap, basemap_sr=basemap_sr, width=width, height=height)
        map.add_raster_layer(self, raster_field=raster_field)
        map.display()

    @staticmethod
    def register_extensions():
        # Add display method to DataFrame for additional convenience
        from pyspark.sql import DataFrame
        DataFrame.display_layer = EsriJSMap.display_layer
        DataFrame.display_raster_layer = EsriJSMap.display_raster_layer

    def __init__(self, *, basemap="gray-vector", basemap_sr=4326, width="100%", height="600px", debug_html=False):
        self.basemap = basemap
        self.basemap_sr = basemap_sr
        self.width = width
        self.height = height
        self.sdk_version = "5.0"
        self.layers = []
        self.raster_layers = []
        self.debug_html = debug_html
        self.div_id = "inline_map_" + str(uuid.uuid4()).split("-")[0]

    def add_raster_layer(self, df, raster_field="raster", colormap=None, max_tiles=1000):

        df = df.select(RT.transform(raster_field, self.basemap_sr).alias("raster"))
        
        df = df.select(RT.extent("raster").alias("extent"), 
                       RT.to_binary("raster", format="tiff", policy="maximum", compression="lzw").alias("bytes"))\
               .select("bytes", ST.min_x("extent"), ST.min_y("extent"), ST.max_x("extent"), ST.max_y("extent"))\
        
        collected = df.take(max_tiles)

        for row in collected:
            bytes = row.bytes

            b64_bytes = base64.b64encode(bytes)
            b64_str = b64_bytes.decode('utf-8')

            colormap_val = "null"
            if colormap:
                if isinstance(colormap, str):
                    colormap_val = dumps({"colorRampName":colormap})
                elif isinstance(colormap, dict):
                    colormap_val = dumps(colormap)
            
            layer_idx = len(self.raster_layers)
            layer_js = f"""
            const raster_layer{layer_idx} = createImageLayer('{b64_str}', {colormap_val})
            map.add(raster_layer{layer_idx});
            """

            self.raster_layers.append({
                "js" : layer_js,
                "extent" : row[1:5]
            })

    def add_layer(self, df, *, renderer=None, label=None, popup=None, max_records=100000, fields=None, **render_args):

        if label is not None:
            if isinstance(label, dict):
                pass
            elif isinstance(label, str):
                label = Labels.field(label)
            else:
                raise ValueError("Unexpected value for 'label'. Expecting a string field name or a dictionary of label properties to pass directly to 'layer.labelingInfo' in Javascript")

        if popup is not None:
            if isinstance(popup, dict):
                pass
            elif isinstance(popup, list | tuple):
                popup = Popups.fields(*popup)
            else:
                raise ValueError("Unexpected value for 'popup'. Expecting a list of string field names or a dictionary of properties to pass directly to 'layer.popupTemplate' in Javascript")

        if renderer is None and render_args:
                # See if we can pull the renderer info out of render_args
                geometry_field = df.st.get_geometry_field()
                if geometry_field:
                    geometry_type = df.schema[geometry_field].dataType.simpleString()
                    if geometry_type == "point" or geometry_type == "multipoint":
                        renderer = Renderers.simple_marker(**render_args)
                    elif geometry_type == "linestring":
                        renderer = Renderers.simple_line(**render_args)
                    elif geometry_type == "polygon":
                        renderer = Renderers.simple_fill(**render_args)
        
        if fields is None:
            fields = []

        field_set = set()
        field_set.update(fields)

        # We do not include all fields by default because the feature data is injected directly into the script source which can 
        # affect browser performance. Map element objects that require fields can list them in the "_references" tag so that we
        # know to include them for proper map rendering.
        for item in [renderer, label, popup]:
            if item is not None:
                field_set.update(item.pop("_references", []))
        
        (features_json, extent) = self._make_feature_layer_js(df, map_sr = self.basemap_sr, fields = list(field_set), max_records = max_records)
        
        self.layers.append({
            "features": features_json,
            "renderer": "null" if renderer is None else dumps(renderer),
            "labelingInfo": "null" if label is None else dumps(label),
            "popupTemplate" : "null" if popup is None else dumps(popup),
            "extent" : extent
        })

    def display(self):
        """
        Generate and display the HTML for the map.
        """
        (layers_js, extent_js) = self._generate_layers_js()
        map_template = Template("""
        <meta name="viewport" content="initial-scale=1,maximum-scale=1,user-scalable=no" />
        <style>
          #$div_id {
            height: $height;
            width: $width;
          }
        </style>
        <script type="module" src="https://js.arcgis.com/5.0/"></script>
        <script type="module">
        
            // Wait until the loader has initialized $$arcgis. Necessary due to how we're injecting scripts into
            // the DOM via cell execution.
            while (!window.$$arcgis) await new Promise(r => setTimeout(r, 25));

            const [FeatureLayer, ImageryTileLayer, Point, Multipoint, Polyline, Polygon] = await $$arcgis.import([
              "@arcgis/core/layers/FeatureLayer.js",
              "@arcgis/core/layers/ImageryTileLayer.js",
              "@arcgis/core/geometry/Point.js",
              "@arcgis/core/geometry/Multipoint.js",
              "@arcgis/core/geometry/Polyline.js",
              "@arcgis/core/geometry/Polygon.js"
            ]);

            const [rasterFunctionUtils] = await $$arcgis.import(["@arcgis/core/layers/support/rasterFunctionUtils.js"])
    
            function createImageLayer(b64, colormap) {
                 const raster_binary = atob(b64);
                 const raster_bytes = new Uint8Array(raster_binary.length);
                 for (let i = 0; i < raster_binary.length; i++) raster_bytes[i] = raster_binary.charCodeAt(i);
                 const raster_blob = new Blob([raster_bytes], { type: "image/tiff" })
                 const raster_blob_url = URL.createObjectURL(raster_blob)
                 var layer = new ImageryTileLayer({
                   url: raster_blob_url,
                   title: "Raster Layer",
                   ioConfig: {
                     datasetFormat: "TIFF"        
                   },
                   interpolation : "nearest"
                 });
                 if (colormap) {
                     console.log("Adding colormap: " + colormap)
                     layer.rasterFunction = rasterFunctionUtils.colormap(colormap);
                 }
                 return layer
             }
                 
            const viewElement = document.querySelector("#$div_id");
            await viewElement.viewOnReady();
            const map = viewElement.map;
            const view = viewElement.view;
            view.extent = $extent_js
            
            $layers_js
        </script>
        <arcgis-map id="$div_id" basemap="$basemap">
          <arcgis-fullscreen slot="top-right"></arcgis-fullscreen>
          <arcgis-zoom slot="top-left"></arcgis-zoom>
        </arcgis-map>
        """)
    
        html = map_template.substitute({
            "sdk_version": self.sdk_version,
            "div_id": self.div_id,
            "basemap": self.basemap,
            "width": self.width,
            "height": self.height,
            "layers_js": layers_js,
            "extent_js": extent_js
        })
        
        display(HTML(html))

        if self.debug_html:
            import textwrap
            md = (
              "# Map HTML\n" +
              "```html\n" +
              textwrap.dedent(html) +
              "```\n"
            )
            display(Markdown(md))

    # Merges the `merge` extent into `target` in place
    @staticmethod
    def _merge_extent(target, merge):
        target[0] = min(merge[0], target[0])
        target[1] = min(merge[1], target[1])
        target[2] = max(merge[2], target[2])
        target[3] = max(merge[3], target[3])
    
    def _make_feature_layer_js(self, df, map_sr, fields, max_records = 100000):
        
        geometry_field = df.st.get_geometry_field()
    
        if geometry_field is None:
            raise ValueError("Unable to determine geometry field")
    
        # Transform features to match map spatial reference
        df = df.withColumn(geometry_field, ST.transform(geometry_field, map_sr))
        
        sr = df.st.get_spatial_reference()
        if sr is None:
            raise ValueError("Spatial reference is required")
    
        selected_attributes = df.select(fields).columns
    
        # Separate geometry from regular attributes
        if geometry_field in selected_attributes:
            selected_attributes.remove(geometry_field)
        
        sr_json = dumps({ "wkid": sr.srid } if sr.srid != 0 else { "wkt": sr.wkt })
    
        geometry_datatype = df.select(geometry_field).schema[0].dataType
        geometry_ctor = geometry_ctor = self._geometry_mapping.get(type(geometry_datatype), None)
        geometry_type = geometry_ctor.lower()
        
        # This is a clunky way to drop the spatial reference from the geometry so that it doesn't get added
        # to the produced JSON for each feature. 
        df = df.withColumn(geometry_field, ST.geom_from_binary(ST.as_binary(geometry_field)))

        # Add OID
        oid_field = "__oid__"
        df = df.withColumn(oid_field, F.monotonically_increasing_id() + 1)
        selected_attributes.append(oid_field)
    
        # Collect rows as JSON
        rows = df.select(
            ST.as_esri_json(geometry_field).alias("geometry_json"),  # geometry
            F.to_json(F.struct(*selected_attributes)).alias("attribute_json"), # attributes
            ST.min_x(geometry_field), ST.min_y(geometry_field), ST.max_x(geometry_field), ST.max_y(geometry_field) # extent
        ).take(max_records)
    
        # Define fields
        fields_json = []
        for field in df.select(selected_attributes).schema:
            type_name = self._type_mapping.get(field.dataType.typeName(), None)
            fields_json.append(dumps({"name": field.name, "type": type_name}))
    
        # Create feature graphics
        feature_template = """{geometry: new """ + geometry_ctor + """(%s), attributes: %s}"""
        extent = [math.nan, math.nan, math.nan, math.nan] # xmin, ymin, xmax, ymax
        features = []
        for row in rows:
            features.append(feature_template % (row[0], row[1]))
            self._merge_extent(extent, row[2:6])
            
        json = """new FeatureLayer({source: [%s], fields: [%s], "geometryType" : "%s", spatialReference: %s, objectIdField:"%s"}) """ % (",".join(features), ",".join(fields_json), geometry_type, sr_json, oid_field)
        return (json, extent)

    
    def _generate_layers_js(self):
        """
        Generate JavaScript code for adding layers to the map.
        """
        layers_js = []
        extent = [math.nan, math.nan, math.nan, math.nan]
        for i, layer in enumerate(self.layers):

            json_string = layer['features']
            
            layer_js = f"""

            const layer{i} = {json_string};

            const renderer{i} = {layer['renderer']}
            if (renderer{i} != null) {{
              layer{i}.renderer = renderer{i}
            }}

            const labelingInfo{i} = {layer['labelingInfo']}
            if (labelingInfo{i} != null) {{
              layer{i}.labelingInfo = labelingInfo{i}
            }}

            const popupTemplate{i} = {layer['popupTemplate']}
            if (popupTemplate{i} != null) {{
              layer{i}.popupTemplate = popupTemplate{i}
            }}

            map.add(layer{i});
            """
            layers_js.append(layer_js)

            self._merge_extent(extent, layer["extent"])

        for layer in self.raster_layers:
            layer_js = layer["js"]
            layers_js.append(layer_js)
            self._merge_extent(extent, layer["extent"])

        # Pad extent by 20%
        pad_width = (extent[2] - extent[0]) * .1
        pad_height = (extent[3] - extent[1]) * .1
        
        extent_js = dumps({"type": "extent", "xmin": extent[0] - pad_width, "ymin": extent[1] - pad_height, "xmax": extent[2] + pad_width, "ymax": extent[3] + pad_height})

        return ("\n".join(layers_js), extent_js)

class Labels:

    @staticmethod
    def arcade(expression):
        return {
            "type": "text", 
            "labelExpressionInfo": { 
                "expression": expression
            }
        }

    @staticmethod
    def field(field):
        return {
            "type": "text", 
            "labelExpressionInfo": { 
                "expression": f"$feature['{field}']"
            },
            "_references": [field]
        }

class Renderers:

    @staticmethod
    def simple_marker(color = "lightblue", outline = None, size = "3px", style = "circle"):

        style_options = ["circle", "square", "cross", "x", "diamond", "triangle", "path"]
        assert style in style_options, f"style must be one of {style_options}"
        
        symbol = { 
            "type": "simple-marker",
            "size" : size,
            "color": color if color else [0,0,0,0],
            "outline" : { "color": outline, "width": 1 } if outline else None,
            "style" : style
        }

        return { "type" : "simple", "symbol" : symbol }

    @staticmethod
    def simple_line(color = "lightblue", width = 2, style="solid"):

        style_options = ["dash", "dash-dot", "dot", "long-dash", "long-dash-dot", "long-dash-dot-dot", "none", "short-dash", "short-dash-dot", "short-dash-dot-dot", "short-dot", "solid"]
        assert style in style_options, f"style must be one of {style_options}"
        
        symbol = { 
            "type" : "simple-line",
            "color" : color if color else [0,0,0,0],
            "width" : width,
            "style" : style
        }

        return { "type" : "simple", "symbol" : symbol }

    
    @staticmethod
    def simple_fill(color="lightblue", outline=None, style="solid"):

        style_options = ["backward-diagonal", "cross", "diagonal-cross", "forward-diagonal", "horizontal", "none", "solid", "vertical"]
        assert style in style_options, f"style must be one of {style_options}"
        
        symbol = { 
            "type": "simple-fill",
            "color": color if color else [0,0,0,0],
            "outline": { "color": outline, "width": 1 } if outline else None,
            "style": style
        }

        return {"type": "simple","symbol": symbol }

class Popups:

    def template(template_json):
        return template_json

    def fields(*fields):
        html = ""
        for field in fields:
            html += f"<b>{field}:</b> {{{field}}}<br/>"

        return {"title": "Fields", "content": html, "_references": fields}

    def fields_table(*fields):
        field_infos = []
        for field in fields:
            field_infos.append(dict(fieldName=field, label=field))

        content = [{
            "type" : "fields",
            "fieldInfos" : field_infos
        }]

        return {"title": "Fields", "content": content, "_references": fields}
