"""
These are some of geoserver's rest api functionalities merged into the class below
Please input your credentials to run already installed geoserver library locally
Please some of the funtionalities might already be deprecated in geoserver repo.

Created:
    O.Aremu
    25.6.2023
"""

import Geoserver
import logging

logger = logging.getLogger(__name__)


class geoserver_fn:

    def __init__(self):

        self.geo = Geoserver(
            "http://localhost:8080/geoserver/",  # input required incase different
            username="",  # input required
            password="",  # input required
        )
        logger.info(self.geo)
        logger.info("Geoserver Object Created")

    def create_workspace(self, wksp_name):
        # For creating workspace
        try:
            self.geo.create_workspace(workspace=wksp_name)

            logger.info(f"Workspace successfully created in Geoserver: {wksp_name}")

        except Exception as er1:
            logger.info(f"Geoserver workspace not created Error: {er1}")

    def del_workspace(self, wspace_name):
        # For deleting workspace
        try:
            self.geo.delete_workspace(workspace=wspace_name)

            logger.info(f"Workspace successfully deleted from Geoserver: {wspace_name}")

        except Exception as er2:

            logger.info(f"Geoserver workspace not deleted Error: {er2}")

    def create_datastore(self, dstore_name, wksp, dbname, hst, pguser, pwd):
        # create_datastore(self,dstore_name,store_name,workspace,db,host,pg_user,pg_password):
        # For creating postGIS connection/datastore
        # Please always use existing workspace name
        try:

            # self.geo.create_featurestore(store_name='',workspace='',
            # db='',host='',pg_user='',pg_password='')
            self.geo.create_featurestore(
                store_name=dstore_name,
                workspace=wksp,
                db=dbname,
                host=hst,
                pg_user=pguser,
                pg_password=pwd,
            )

            logger.info(f"Datstore successfully created in Geoserver: {dstore_name}")

        except Exception as er3:
            logger.info(f"Geoserver datastore not created Error: {er3}")

    def create_raster_layer(self, titl_name, pfad, wkp):
        # For publishing/ uploading raster data from local to the geoserver
        # geo.create_coveragestore(layer_name='layer1',
        # path=r'C:\path\to\rasterdatenhere', workspace='demo')
        try:

            self.geo.create_coveragestore(
                layer_name=titl_name, path=pfad, workspace=wkp
            )

            logger.info(f"Raster Layer from local created in Geoserver: {titl_name}")

        except Exception as er4:

            logger.info(f"Geoserver create shape layer Error: {er4}")

    def create_shp_layer(self, wks, strnam, pgtab, tit):
        # self.geo.publish_featurestore(workspace='', store_name='',pg_table='',title='')
        # For publishing layer from db to geoserver
        # Default styles are allocated to uploaded layers from Geoserver automatically
        try:
            self.geo.publish_featurestore(
                workspace=wks, store_name=strnam, pg_table=pgtab, title=tit
            )

            logger.info(f"Shape Layer created in Geoserver: {tit}")
        except Exception as er5:

            logger.info(f"Geoserver create shape layer Error: {er5}")

    def create_shp_style(self, shp_pfad, wks):
        # For uploading SLD file from local computer into geoserver
        # self.geo.upload_style(path=r'path\to\sld\file.sld', workspace='')
        # Stylename will automatically be taken from the .sld file name

        try:

            self.geo.upload_style(path=shp_pfad, workspace=wks)

            logger.info(
                f"Shapefile-layer Style successfully created in Geoserver using: {shp_pfad}"
            )

        except Exception as er6:

            logger.info(f"Shapefile-layer style create Error: {er6}")

    def allocate_style_to_shp(self, shp_name, sty_name, wks):
        # geo.publish_style(layer_name='geoserver_layer_name', style_name='', workspace='')

        try:
            self.geo.publish_style(
                layer_name=shp_name, style_name=sty_name, workspace=wks
            )

            logger.info(
                f"Shapefile Style successfully created in Geoserver: {sty_name}"
            )

        except Exception as er7:
            logger.info(f"Error allocate_style_to_shp : {er7}")

    # Define Raster style
    def create_raster_style(self, local_pfad, sty_name, wkp, color_hue=None):
        # For creating the style file for raster data dynamically and connect it with layer
        # self.geo.create_coveragestyle(raster_path=r'path\to\raster\file.tiff', style_name='style_1', workspace='',color_ramp='RdYiGn')

        color_hue = "RdYiGn"
        try:
            self.geo.create_coveragestyle(
                raster_path=local_pfad,
                style_name=sty_name,
                workspace=wkp,
                color_ramp=color_hue,
            )
            logger.info(f"Raster Style successfully created in Geoserver: {sty_name}")

        except Exception as er8:
            logger.info(f"Raster style create Error: {er8}")
            logger.info(f"Error create_raster_style : {er8}")

    def allocate_style_to_raster(self, rast_name, st_nam, wks):
        # For giving already published raster layer style conventions with already existing styles
        # geo.publish_style(layer_name='geoserver_layer_name', style_name='raster_file_name', workspace='')

        try:
            self.geo.publish_style(
                layer_name=rast_name, style_name=st_nam, workspace=wks
            )

            logger.info(f"{st_nam} allocated to {rast_name} in geoserver")

        except Exception as er9:

            logger.info(f"Error {st_nam} not allocated to {rast_name} in geoserver")
            logger.info(f"Error allocate_style_to_raster : {er9}")

    def del_layer(self, ly_call, wks):
        # For deleting layer
        # self.geo.delete_layer(layer_name=ly_call, workspace='')
        try:
            self.geo.delete_layer(layer_name=ly_call, workspace=wks)

            logger.info(f"Layer deleted from Geoserver: {ly_call}")

        except Exception as er10:

            logger.info(f"Geoserver delete layer Error: {er10}")
            logger.info(f"Error del_layer : {er10}")

    def del_style(self, sty_name, wks):
        # For deleting style
        # self.geo.delete_style(style_name=style_name, workspace='')
        try:
            self.geo.delete_style(style_name=sty_name, workspace=wks)
            logger.info(f"Style successfully deleted from Geoserver: {sty_name}")

        except Exception as er11:

            logger.info(f"Geoserver del_style Error: {er11}")
            logger.info(f"Error del_layer : {er11}")


# Class caller
# geoserver_fn().create_shp_style(shp_pfad= '/path/to/sld/crazy.sld',wks= 'geonode')
# geoserver_fn().del_layer(ly_call='Weisse Flecken P3 - F1637 V325m10',wks='geonode')
