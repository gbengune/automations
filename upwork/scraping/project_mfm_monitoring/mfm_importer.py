"""
Created on: 1.07.2024
Created by: O.Aremu
Warning: To be used by Linux Users only.
Tasks:
    Handles project processing.
1.)SCrapes project Data website for data
2.) It checks each table online to see if there is a newer bundesland table
    to that already registered in the bridge table
3. If a newer table is not found it skips the bundesland, else if a newer
    table is found online, then it goes on to create a main-folder
4.) Downloads the csv file
5.) Creates a sub-folder and drops the unpacked csv inside
6.) Creates a table and imports the unpacked csv into the table in the DB
7.) Further inserts the contents from the DB into the global table.
8.) Deleted the sub-folder from the local system
9.) Deletes the single table from the DB
10.) Updates the timestamp for the Bundesland in the bridge for next quartals'
    data import
11.) Then repeats the processes from 2 to 10 für the coming states (Bundesland))

"""


from path.to.python.module.pythonmodulename import (
    mfm_queries as query,
)
import shutil
import os
import sys
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
import glob
import logging
from airflow.hooks.postgres_hook import PostgresHook


logger = logging.getLogger(__name__)


class mfm_importer:
    def __init__(self):
        try:
            self.target_db_conn = PostgresHook(postgres_conn_id="***")
            self.conn = self.target_db_conn.get_conn()
            self.cur = self.conn.cursor()

        except Exception as er1:
            logger.exception(f"[Error] on Level 1 : {er1}")

        # Defining global variables
        try:
            # define website
            self.website = "https://start.html"

            # define directory
            self.dir = os.getcwd()  # already set as default in backend

            # defining expected response
            self.response = 200

            # parser to be used
            self.parser = "html.parser"

            # defining file extensions
            self.ext = "zip"

            # declare class variables
            self.html_tag = "a"
            self.html_class = "downloadLink Publication FTzip"
            self.html_class_container = "href"

            # declare schema name
            self.schema = "***"
            # declare tab names
            self.tables = ["project_mfm", "project_mfm_bridge"]

            # defining split delimiter
            self.prefix = "mfm"
            self.name_splitter = "/"
            self.name_splitter_b = "_"
            self.name_splitter_c = "-"
            self.csv_sep = ","

            # defining local variables
            self.folder = "project"
            self.folder_zipped = "extracted"

        except Exception as er2:
            logger.exception(f"[Error] on Level 2 : {er2}")

    # ENTRY POINT
    def return_is_website_valid(self):
        """
        Mathod checks to see if website is valid
        :return:
        """
        url_link = self.website
        try:
            is_valid = requests.get(url=url_link)

            if is_valid.status_code == self.response:
                logger.info("[Response]: Website Valid...advancing")

                # create local folder one time for processing
                self.create_folder(fd_name=self.folder)

                # start the processes
                self.get_sitecontent(site=url_link)

            else:
                logger.debug("Website Invalid...ending")
                sys.exit()
        except Exception as er3:
            logger.exception(f"[Error] on Level 3 : {er3}")

    def get_sitecontent(self, site):
        try:
            response = requests.get(site)

            # hardcoded:  text conversion
            content = response.text

            # calling parser with beatifulsoup
            self.parse_site(urllink=content, url_parser=self.parser)

        except Exception as er4:
            logger.exception(f"[Error] on Level 4 : {er4}")

    def parse_site(self, urllink, url_parser):
        try:
            # parsing with Soup
            obj_content = BeautifulSoup(urllink, url_parser)

            self.bsp_site_contents(obj=obj_content)

        except Exception as er5:
            logger.exception(f"[Error] on Level 5 : {er5}")

    def bsp_site_contents(self, obj):
        try:
            data_classes = obj.find_all(self.html_tag, class_=self.html_class)

            """checking to see if class tags exists in html content.
               This will catch website changes in the class definitons of data contents

            """

            is_exists_html_class = list(data_classes)

            if is_exists_html_class:
                logger.info("[Valid]: html_class kpi for website valid...continuing")
                self.filter_site_contents(contents=data_classes)
            else:
                logger.info(
                    "[Invalid]: html_class kpi for website invalid, change in project definition"
                )
                sys.exit()

        except Exception as er6:
            logger.exception(f"[Error] on Level 6 : {er6}")

    def filter_site_contents(self, contents):
        """here, all data content tags will be filtered out of all website contents
        This are the website datasets needed"""
        try:
            for data in contents:
                content_row = data.get(
                    self.html_class_container
                )  # getting Html-Tag attribute (href)

                # loop through db row and match rows to found website contents
                self.get_tab_kpis_from_db(input_content=content_row)
                logger.info("----------")

        except Exception as er7:
            logger.exception(f"[Error] on Level 7 : {er7}")

    # compared website data with data from bridge table in database
    def get_tab_kpis_from_db(self, input_content):
        try:
            # helps acquire key-row level kpis for each German State(Bundesland) in database
            sql = query.sql_get_laender

            self.cur.execute(sql.format(schema=self.schema, table=self.tables[1]))
            res_tab = self.cur.fetchall()

            # loop through bridge table row by row
            for tabs in res_tab:
                """if date in site content does not match date in bridge,
                do nothing skip, assuming date in site content is always
                updated by website owner after each new data upload.

                """

                """matches kpi bundesland in bridge table
                had to use .zip in expression matching  due to issues  with
                Sachsen and Sachsen-Anhalt """

                kennung = tabs[0]
                bundesland = tabs[1]  # means e.g berlin
                germ_state = (
                    tabs[1] + tabs[2]
                )  # means e.g Berlin.zip, done becuz of sachsen/sachsen_anhalt matching'
                upload_date = tabs[3]  # means e.g 202401

                # if germ_state == 'Baden-Wuerttemberg.zip':

                # matching expressions form bridge to website content links
                # if data does not already exists import else ignore
                if germ_state in input_content and upload_date not in input_content:
                    logger.info(
                        f"[New bride-Content found in Website] for : {bundesland}"
                    )
                    logger.info(f"[Link]: {input_content}")
                    logger.info(f"[Bridge-Output] : {tabs}")
                    name_rule = self.return_as_db_tab_names(name=input_content)

                    """create for each bundesland-loop, a sub-folder in
                        previously created local folder for storing all extracted files
                        """
                    self.create_folder(
                        fd_name=os.path.join(self.folder, self.folder_zipped)
                    )

                    # downloading first-return website content
                    self.downloader(
                        link=input_content,
                        folder=f"{self.dir}/{self.folder}",
                        doc_name=name_rule,
                    )

                    # extracting into sub folder
                    self.csv_extractor(
                        fl_folder=os.path.join(
                            self.dir, self.folder, self.folder_zipped
                        ),
                        fl_name=os.path.join(self.dir, self.folder, name_rule),
                        fl_extension=self.ext,
                    )

                    # create table skeleton in db
                    # Note: waits till append ends outside of the loop
                    table_columns = self.return_datatypes(
                        subfolder=os.path.join(
                            self.dir, self.folder, self.folder_zipped
                        ),
                        d_sep=self.csv_sep,
                    )

                    self.cr_db_table(
                        tab_schema=self.schema,
                        tab_name=name_rule,
                        tab_cols=table_columns,
                        online_link=input_content,
                    )

                    self.csv_in(
                        tb_schema=self.schema,
                        tbl_name=name_rule,
                        subfolder=os.path.join(
                            self.dir, self.folder, self.folder_zipped
                        ),
                        sep_col=self.csv_sep,
                    )

                    # insertion of currently imported table in global table
                    self.get_table_cols(
                        global_sch=self.schema,
                        global_tab=self.tables[0],
                        current_table=name_rule,
                        current_table_date=self.current_datum(
                            content_date=input_content
                        ),
                        bundesland_key=kennung,
                    )

                    # delete sub-folder at every bundesland loop end
                    self.delete_csv(
                        subfolder=os.path.join(
                            self.dir, self.folder, self.folder_zipped
                        )
                    )

                    # delete  current table in db
                    self.del_table(sch=self.schema, tab=name_rule)

                    # update timestamp of bundesland in bridge table
                    self.update_datum(
                        sch=self.schema,
                        tab=self.tables[1],
                        up_datum=self.current_datum(content_date=input_content),
                        bundes_land=bundesland,
                    )  # means tab[0]

                elif germ_state in input_content and upload_date in input_content:
                    logger.info(f"[Content found already exists] : {bundesland}")
                    logger.info(f"[No import required] for : {bundesland}")
                    logger.info(f"[Link]: {input_content}")
                    logger.info(f"[Bridge-Content] : {tabs}")

        except Exception as er8:
            logger.exception(f"[Error] on Level 8 : {er8}")

    def downloader(self, link, folder, doc_name):
        try:
            # filenames will be switched for adapting database naming conventions
            dataset = requests.get(link)

            with open(f"{folder}/{doc_name}", "wb") as copy:
                copy.write(dataset.content)
                copy.close()
                logger.info(f"[File] {doc_name} downloaded into {folder}")

        except Exception as er9:
            logger.exception(f"Error on level 9 : {er9}")
            sys.exit()

    def return_as_db_tab_names(self, name):
        # converting html content names to database naming convention
        if isinstance(name, str) is True:
            lv1 = name.split(self.name_splitter)
            lv2 = lv1[-1]
            lv3 = lv2.split(f".{self.ext}")
            lv4 = lv3[0]
            lv5 = lv4.split(self.name_splitter_b)

            # hard coded
            a = self.prefix
            b = lv5[2].replace(self.name_splitter_c, self.name_splitter_b).lower()
            c = lv5[0]

            lv6 = f"{a}_{b}_{c}"

            logger.info(lv6)
            return lv6
        else:
            logger.info(f"String value expected but got {type(name)} for {name}")
            sys.exit()

    def create_folder(self, fd_name):
        # Used to create main (zips) and subfolder (extracted csv files)
        fd = f"{self.dir}/{fd_name}"
        fd_exists = os.path.exists(path=fd)

        if fd_exists is True:
            com_drop = f"rm -r {fd}"
            # executed like from linux terminal
            os.system(com_drop)
            logger.info(f"[Folder used for last Upload Dropped] : {fd}")
            logger.info(f"[Folder is being Recreated] : {fd}")
            os.mkdir(fd)
            logger.info(f"[Folder Recreated] : {fd}")
            return fd

        elif fd_exists is False:
            os.mkdir(fd)
            logger.info(f"Folder Created : {fd}")
            return fd

    def csv_extractor(self, fl_folder, fl_name, fl_extension):
        # unpacking the csvs from sub folders
        try:
            shutil.unpack_archive(
                filename=fl_name, extract_dir=fl_folder, format=fl_extension
            )
            logger.info(f"[Extracted/Unpacked] into sub folder: {fl_name}")

        except Exception as er10:
            logger.exception(f"[Error] on level 10 : {er10}")
            sys.exit()

    def cr_db_table(self, tab_schema, tab_name, tab_cols, online_link):
        try:
            # creates db tables using online data attributes
            sql = query.sql_cr_tabs

            self.cur.execute(
                sql.format(
                    schema=tab_schema,
                    table=tab_name,
                    columns=tab_cols,
                    zip_file=online_link,
                )
            )
            self.conn.commit()
            logger.info(f"[Table created] in DB : {tab_name}")

        except Exception as er11:
            logger.exception(f"[Error] on Level 11 : {er11}")

    def return_datatypes(self, subfolder, d_sep):
        # used for reading csv formats to get columns used for table creation
        try:
            file_b = glob.glob(f"{subfolder}/*.csv")

            for csv in file_b:
                dt = pd.read_csv(csv, index_col=None, sep=d_sep)
                # for creating table columns
                att = dt.dtypes
                att_c = []
                att_c.clear()

                for c, d in att.items():
                    # making content length for integers all int8 etc.
                    att_c.append(
                        '"'
                        + c
                        + '"'
                        + " "
                        + re.sub("64", "8", str(d)).replace("object", "varchar(100)")
                    )

                    # columns for table creation
                    att_d = ",".join(att_c)
                # waits till append ends for all columns
                return att_d

        except Exception as er12:
            logger.exception(f"[Error] on level 12 : {er12}")
            sys.exit()

    def delete_csv(self, subfolder):
        """
        this method deletes copied out row entries from server folder
        """

        try:
            file_b = glob.glob(f"{subfolder}/*.csv")

            for csv in file_b:
                if os.path.exists(csv) is True:
                    logger.info(f"[Removing File] from localtree : {csv}")
                    command = f"rm -r {subfolder}"
                    os.system(command)
                    logger.info(f"[Removed File] from localtree : {csv}")

                else:
                    logger.debug(f"[File does not exist in localtree] : {csv}")

        except Exception as er13:
            logger.exception(f"Error on Level 13: {er13}")
            sys.exit()

    def csv_in(self, tb_schema, tbl_name, subfolder, sep_col):
        try:
            sql_copy_in = f"COPY {tb_schema}.\"{tbl_name}\" from STDIN WITH CSV DELIMITER '{sep_col}' NULL '' HEADER"

            # not limited to one csv , but list is always cleared at end of each file loop
            file_b = glob.glob(f"{subfolder}/*.csv")
            logger.info(file_b)
            for csv in file_b:
                with open(csv, "r") as r_file:
                    self.cur.copy_expert(sql_copy_in, r_file)
                    self.conn.commit()
                    logger.info(
                        f"[File Inserted in Table] {tbl_name} from: {subfolder}"
                    )

        except Exception as er14:
            logger.exception(f"Error on Level 14: {er14}")
            sys.exit()

    def update_datum(self, sch, tab, up_datum, bundes_land):
        try:
            # updates bridge table date entry for each row
            sql = query.sql_update_datum
            self.cur.execute(
                sql.format(
                    schema=sch, table=tab, datum=up_datum, german_state=bundes_land
                )
            )
            self.conn.commit()
            logger.info("[Timestamp] updated in bridge table")

        except Exception as er15:
            logger.exception(f"[Error] on Level 15 : {er15}")

    def current_datum(self, content_date):
        # gets datasets release date entry form dataset's name
        if isinstance(content_date, str) is True:
            xy = content_date.split("/")[-1]
            xyz = xy.split("_")[0]

            return xyz
        else:
            logger.debug(f"[Expected] string for Datum entry, got {type(content_date)}")

    # -------------- Global Table processes below

    def get_table_cols(
        self, global_sch, global_tab, current_table, current_table_date, bundesland_key
    ):
        # extracts current table columns and uses it for insert into global table
        try:
            # gets columns belonging to table in the db
            sql = query.sql_table_entity
            self.cur.execute(sql.format(schema=global_sch, table=current_table))
            tb_attr = self.cur.fetchall()
            logger.info(f"[Accessed] {current_table} for global processing")

            for var in tb_attr:
                # s_tb_sch = var[0]  # s means source. current table schema
                s_tb_tab = var[1]  # current table name
                s_tb_cols = var[2]  # current table columns

                # insert in global table here for each table instance
                self.insert_in_tab_global(
                    d_schema=global_sch,  # 'roh',
                    d_tab=global_tab,  # 'mfm_project_global',
                    d_cols=s_tb_cols,
                    source_table=s_tb_tab,
                    actual_stand=True,  # column actual
                    content_date=current_table_date,  # '202404',
                    id_partition=bundesland_key,
                )  # from 1 to 16 Bundeslaender

        except Exception as er16:
            logger.exception(f"Error on Level 16: {er16}")

    def insert_in_tab_global(
        self,
        d_schema,
        d_tab,
        d_cols,
        source_table,
        actual_stand,
        content_date,
        id_partition,
    ):
        try:
            # inserts features from current table into global table

            logger.info(f"[Inserting] features for {source_table} in global table")

            sql = query.sql_insert_in_global
            self.cur.execute(
                sql.format(
                    schema=d_schema,
                    table=d_tab,
                    columns=d_cols,
                    s_table=source_table,
                    stand=actual_stand,
                    data_date=content_date,
                    partition_id=id_partition,
                )
            )
            self.conn.commit()
            logger.info(f"[Inserted] features for {source_table} in global table")

        except Exception as er17:
            logger.exception(f"[Error] on Level 17 : {er17}")

    def del_table(self, sch, tab):
        try:
            sql = query.sql_delete_table

            self.cur.execute(sql.format(schema=sch, table=tab))
            self.conn.commit()
            logger.info(f"[Deleted] table : {tab}")

        except Exception as er18:
            logger.exception(f"[Error] on Level 18 : {er18}")

# triggered by Airflow Dag
def trigger_project_scraper():
    mfm_importer().return_is_website_valid()
    logger.info("--------------------------")
