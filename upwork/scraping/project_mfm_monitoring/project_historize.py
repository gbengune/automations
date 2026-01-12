"""
Created on: 15.07.2024
Created by: O.Aremu

Tasks:
    1.) Handles projecta Historisation.

    2.) If a new column is introduced from website,
        please add it to vor and nach aliases in sql_historize.
        Also add it to  the main table projecta_mfm

    3.) does timeseries change of the inputs in columns change_from and change_to
    4.) does change in time delivery of data in columns valid_from and valid_to
"""

import logging
from datetime import datetime
import sys
from airflow.hooks.postgres_hook import PostgresHook

from prod.src.projects.op.projecta_mfm_monitoring import (
    projecta_historize_queries as query,
)

logger = logging.getLogger(__name__)


class projecta_historise:
    def __init__(self):
        try:
            # airflow database connection id (psycopg2 class)
            self.target_db_conn = PostgresHook(postgres_conn_id="***")
            self.conn = self.target_db_conn.get_conn()
            self.conn.autocommit = True
            self.cur = self.conn.cursor()

            # Defining properties
            self.schema = "***"
            self.global_table = "projecta_mfm"
            self.suffix = "projecta_mfm_"
            self.filter = "bridge"

            # timestamp for current id
            self.current_tsp = "999999"

            # timestamp for id values which were previously non-existent.
            self.new_tsp = "888888"

            # current valid_to tsp
            self.tsp_valid_to = "9999"

            # Dagname
            self.dag = "projecta_mfm_website_scraper"
            self.site = "https://start.html"

        except Exception as er1:
            logger.exception(f"[Error] on Level 1 : {er1}")
            sys.exit()

    # ENTRY POINT
    def ignition_starter(self):
        # starts the whole process if new datasets found in subset table
        try:
            sql = query.sql_ignition

            self.cur.execute(sql)
            on_off = self.cur.fetchall()

            if on_off:
                logger.info(
                    "[Status] : New entries found in table projecta_mfm...historising"
                )

                # call method for timeframe update(change_from and change_to)
                self.start_historization()
                logger.info("--------")

                # call method for timeframe update(valid_from/to)
                self.last_update_valid_from_to()
                logger.info("--------")

                # generate metadata only after historisation of global table
                self.create_metadata(sch=self.schema, tab=self.global_table)
                logger.info("--------")

                # call method to comment the table
                self.create_comment(
                    sch=self.schema,
                    tab=self.global_table,
                    date=datetime.now(),
                    dag=self.dag,
                    website=self.site,
                )
                logger.info("--------")

                logger.info("Historisation Process finished")

            else:
                logger.info(
                    "[Status] : No new entries found in table projecta_mfm...shutdown"
                )
                sys.exit()

        except Exception as er2:
            logger.exception(f"[Error] on Level 2 : {er2}")
            sys.exit()

    def start_historization(self):
        dates = self.get_dates()
        pair_dates = self.data_pairer(daten=dates, pairs=2, steps=1)

        logger.info(f"[All Dates] :\n{pair_dates}")
        logger.info("--------")

        state_tables = self.get_tables(
            sch=self.schema, tab_type=self.suffix, tb_filt=self.filter
        )

        logger.info(f"[All Tables] :\n{state_tables}")
        logger.info("--------")

        for date in pair_dates:  # selecting each date-pair
            if len(date) == 2:
                """
                For example: [202107, 202110], [202110, 202201]
                for liste date-pairs with even length (e.g 2,4,6,8)
                In this case, column change_to will be updated for
                    list1- 202110 using comparison results from 202107
                    list2- 202201 using comparison results from 202110
                There will always be a last date in the list, which will be current date
                """
                vor_date = date[0]
                nach_date = date[1]

                logger.info(f"[Processing Date] : {date}")
                logger.info("-----------------------------")

                # selecting each table for historisation in above date
                for state_table in state_tables:
                    """----- checking for attribute change from previous
                    dates of date pairs."""
                    logger.info(
                        f"[Historizing inter Attribute changes] between : {date} in {state_table}"
                    )

                    self.inter_date_changes_hist(
                        sch=self.schema,
                        bund_table=state_table,
                        date_before=vor_date,
                        date_after=nach_date,
                    )

                    # ---- identifying fresh new id rows in new dataset
                    logger.info(
                        f"[Allocating Code 888888 to new ID rows] in date : {nach_date}"
                    )

                    self.upt_new_id_entries(
                        sch=self.schema,
                        tab=state_table,
                        tsp_new_id=self.new_tsp,
                        date_after=nach_date,
                    )

                    """ ----- removing change_to=999999 and setting actual=False
                                from previously current datasets"""
                    logger.info(
                        f"[Changing actualvalue for ID rows previously 999999] in date : {vor_date}"
                    )

                    self.upt_old_actuals(
                        sch=self.schema,
                        tab=state_table,
                        date_before=vor_date,
                        tsp_old=self.current_tsp,
                    )

                    """ ------ setting actual=False where 888888 from
                        previously non existing id rows of current
                                     datasets"""
                    logger.info(
                        f"[Changing actualvalue for new ID rows previously 888888] : {vor_date}"
                    )

                    self.upt_nonexisting(
                        sch=self.schema,
                        tab=state_table,
                        tsp_new=self.new_tsp,
                        date_before=vor_date,
                    )

                    logger.info("--------")

            elif len(date) == 1:  # second historization of most current date
                """In the List, there will always be a single length date string at position (-1)
                For example: [202401]
                Here, the current last actual change_from import date [202310] will get change_to='999999'
                """
                logger.info("--------")

                current_date = date[0]

                logger.info(
                    f"[Phase2: Re-Processing New Current Date] in date : {date}"
                )
                logger.info("-----------------------------")

                for (
                    state_table
                ) in (
                    state_tables
                ):  # selecting each table for historisation in current date
                    # --- allocating change_to=9999 and actual=true to new dataset

                    logger.info(
                        f"[Phase2: Re-Historizing New Current Dataset] for : {date} in {state_table}"
                    )

                    # ---- allocating actual=True and change_to=999999 to
                    #       newest dataset entry

                    self.current_date_changes_hist(
                        sch=self.schema,
                        bund_table=state_table,
                        cur_date=current_date,
                        cur_datum=self.current_tsp,
                    )
                    logger.info("--------")

                    # -------------- allocating all new non-existing ids(888888) with actual=True
                    self.current_date_changes_hist_new(
                        sch=self.schema,
                        bund_table=state_table,
                        cur_date=current_date,
                        new_ids_=self.new_tsp,
                    )
                    logger.info("--------")

                    # -------------- allocating all new non-changing rows with actual=True
                    self.current_date_changes_hist_unchanged(
                        sch=self.schema,
                        bund_table=state_table,
                        cur_date=current_date,
                        cur_datum=self.current_tsp,
                        new_ids_=self.new_tsp,
                    )
                    logger.info("--------")

            else:
                logger.debug("[Report] : No dates found for historising")
                sys.exit()

    def inter_date_changes_hist(self, sch, bund_table, date_before, date_after):
        try:
            sql = query.sql_historize

            logger.info(f"[Start] : {datetime.now()}")
            logger.info(
                f"[Historizing Timeframe] : {date_before}<-->{date_after} for State {bund_table}"
            )

            self.cur.execute(
                sql.format(
                    schema=sch,
                    table=bund_table,
                    vor_datum=date_before,
                    nach_datum=date_after,
                )
            )
            rows_updated = self.cur.rowcount

            logger.info(
                f"[Historized Timeframe] : {date_before}<-->{date_after} for State {bund_table}"
            )
            logger.info(f"[No of Rows Affected] : {rows_updated} rows updated")
            logger.info(f"[End] : {datetime.now()}")
            logger.info("---")

        except Exception as er3:
            logger.exception(f"[Error] on Level 3 : {er3}")
            sys.exit()

    def upt_new_id_entries(self, sch, tab, tsp_new_id, date_after):
        try:
            logger.info(f"[Start] : {datetime.now()}")
            # updates new id rows with '888888'
            sql = query.sql_update_new_ids

            self.cur.execute(
                sql.format(
                    schema=sch,
                    table=tab,
                    nach_datum=date_after,
                    tsp_new_id_input=tsp_new_id,
                )
            )
            rows_updated = self.cur.rowcount
            logger.info(f"[No of Rows Affected] with 888888 : {rows_updated}")

            logger.info(f"[Allocated Code 888888 to new ids] in date : {date_after}")

            logger.info(f"[End] : {datetime.now()}")

            logger.info("---")

        except Exception as er4:
            logger.exception(f"[Error] on Level 4 : {er4}")
            sys.exit()

    def upt_old_actuals(self, sch, tab, tsp_old, date_before):
        try:
            logger.info(f"[Start] : {datetime.now()}")

            # updates old current id rows which have values 'NULL'
            sql = query.sql_update_old_actual

            self.cur.execute(
                sql.format(
                    schema=sch,
                    table=tab,
                    vor_datum=date_before,
                    tsp_old_current=tsp_old,
                )
            )
            rows_updated = self.cur.rowcount
            logger.info(f"[No. of Rows Affected] : {rows_updated}")

            logger.info(f"[Replaced past current Rows 999999] of : {date_before}")

            logger.info(f"[End] : {datetime.now()}")

            logger.info("---")

        except Exception as er5:
            logger.exception(f"[Error] on Level 5 : {er5}")
            sys.exit()

    def upt_nonexisting(self, sch, tab, tsp_new, date_before):
        try:
            logger.info(f"[Start] : {datetime.now()}")

            # updates old current id rows which have values 'NULL'
            sql = query.sql_reset_actual

            self.cur.execute(
                sql.format(
                    schema=sch,
                    table=tab,
                    vor_datum=date_before,
                    tsp_new_id_input=tsp_new,
                )
            )
            rows_updated = self.cur.rowcount
            logger.info(f"[No. of rows Affected] with past 888888 : {rows_updated}")

            logger.info(
                f"[Replaced  past actualvalue for Nonexisting Rows] : {date_before}"
            )

            logger.info(f"[End] : {datetime.now()}")

            logger.info("---")

        except Exception as er6:
            logger.exception(f"[Error] on Level 6 : {er6}")
            sys.exit()

    def current_date_changes_hist(self, sch, bund_table, cur_date, cur_datum):
        """handles last date which is has no date pair, the most recent date
        will usually not have a date pair when number of dates is uneven"""
        try:
            sql = query.sql_update_current

            logger.info(f"[Start] : {datetime.now()}")
            logger.info(
                f"[Setting to Actual] Date {cur_date} :  for State {bund_table}"
            )

            self.cur.execute(
                sql.format(
                    schema=sch,
                    table=bund_table,
                    current_datum=cur_date,
                    def_datum=cur_datum,
                )
            )
            rows_updated_ = self.cur.rowcount

            logger.info(
                f"[Current Dataset Set to Actual (changed)] {cur_date} :  for State {bund_table}"
            )
            logger.info(f"[No. of Rows Affected] : {rows_updated_} rows updated")
            logger.info(f"[End] : {datetime.now()}")

            logger.info("---")

        except Exception as er7:
            logger.exception(f"[Error] on Level 7 : {er7}")
            sys.exit()

    def current_date_changes_hist_new(self, sch, bund_table, cur_date, new_ids_):
        """handles last date which is has no date pair, the most recent date
        will usually not have a date pair."""
        try:
            sql = query.sql_update_current_new

            logger.info(f"[Start] : {datetime.now()}")
            logger.info(
                f"[Setting to Actual for new ids] Date {cur_date} :  for State {bund_table}"
            )

            self.cur.execute(
                sql.format(
                    schema=sch,
                    table=bund_table,
                    current_datum=cur_date,
                    tsp_new_id_input=new_ids_,
                )
            )
            rows_updated_ = self.cur.rowcount

            logger.info(
                f"[Current Dataset Set to Actual (new ids)] {cur_date} :  for State {bund_table}"
            )
            logger.info(f"[No. of Rows Affected] : {rows_updated_} rows updated")
            logger.info(f"[End] : {datetime.now()}")

            logger.info("---")

        except Exception as er8:
            logger.exception(f"[Error] on Level 8 : {er8}")
            sys.exit()

    def current_date_changes_hist_unchanged(
        self, sch, bund_table, cur_date, cur_datum, new_ids_
    ):
        """handles last date which is has no date pair, the most recent date
        will usually not have a date pair when number of dates is uneven"""
        try:
            sql = query.sql_update_current_unchanged

            logger.info(f"[Start] : {datetime.now()}")
            logger.info(
                f"[Setting to Actual] Date {cur_date} :  for State {bund_table}"
            )

            self.cur.execute(
                sql.format(
                    schema=sch,
                    table=bund_table,
                    current_datum=cur_date,
                    def_datum=cur_datum,
                    tsp_new_id_input=new_ids_,
                )
            )
            rows_updated_ = self.cur.rowcount

            logger.info(
                f"[Current Dataset Set to Actual (unchanged)] {cur_date} :  for State {bund_table}"
            )
            logger.info(f"[No. of Rows Affected] : {rows_updated_} rows updated")
            logger.info(f"[End] : {datetime.now()}")

            logger.info("---")

        except Exception as er9:
            logger.exception(f"[Error] on Level 9 : {er9}")
            sys.exit()

    def get_dates(self):
        # here to be processed dates are gotten for pairing
        try:
            sql = query.sql_get_dates

            self.cur.execute(sql.format(default_tsp=self.current_tsp))
            aus = self.cur.fetchall()

            dt = []

            for at in aus:
                for am in at:
                    dt.append(am)

            return dt

        except Exception as er10:
            logger.exception(f"[Error] on Level 10 : {er10}")
            sys.exit()

    def get_tables(self, sch, tab_type, tb_filt):
        # here to be processed partitioned tables are gotten
        try:
            sql = query.sql_get_tables

            self.cur.execute(
                sql.format(schema=sch, tables_like=tab_type, filters=tb_filt)
            )
            out = self.cur.fetchall()

            tables = []
            for tabs in out:
                for tb in tabs:
                    tables.append(tb)

            return tables

        except Exception as er11:
            logger.exception(f"[Error] on Level 11 : {er11}")
            sys.exit()

    def data_pairer(self, daten, pairs: int, steps: int) -> list:
        """
        1. this method pairs up dates (change_from) in ranges for processing
        2. Value in (change_from) dates should never be null otherwise logic will fail.
        """
        try:
            data = [int(g) for g in daten]  # change entries to int
            data.sort(reverse=False)

            logger.info("--------")

            res = list(data[x : x + pairs] for x in range(0, len(data), steps))

            logger.info("--------")
            return res

        except Exception as er12:
            logger.exception(f"[Error] on Level 12 : {er12}")
            sys.exit()

    def create_metadata(self, sch, tab):
        logger.info("[Metadata] : Starting Metadata Generation")

        try:
            logger.info(f"[Start] : {datetime.now()}")

            sql = query.sql_insert_metadata

            self.cur.execute(sql.format(schema=sch, table=tab))
            logger.info(f"[Metadaten] : Metadata generated for {tab}")

            logger.info(f"[End] : {datetime.now()}")

        except Exception as er13:
            logger.exception(f"[Error] on Level 13 : {er13}")
            sys.exit()

    def create_comment(self, sch, tab, date, dag, website):
        logger.info("f[Comment] : Commenting table {tab}")

        try:
            sql = query.sql_insert_comment

            self.cur.execute(
                sql.format(
                    schema=sch, table=tab, datum=date, dagid=dag, projecta_site=website
                )
            )
            logger.info("f[Comment] : Comment added to table {tab}")

        except Exception as er14:
            logger.exception(f"[Error] on Level 13 : {er14}")
            sys.exit()

    def last_update_valid_from_to(self):
        try:
            """this method updates columns valid_from and valid_to independently
            after all hostirisations have been carried out.."""

            logger.info("--------")
            logger.info("[Updating Valid_from | Valid_to]")

            sql = query.valid_from_to_dates
            sql2 = query.valid_from_to_dates_update

            self.cur.execute(sql)
            f_dates = self.cur.fetchall()

            # turn dates to list datatypes
            arr_dats = [",".join(dts) for dts in f_dates]

            date_pairs = self.data_pairer(daten=arr_dats, pairs=2, steps=1)

            states_tables = self.get_tables(
                sch=self.schema, tab_type=self.suffix, tb_filt=self.filter
            )

            logger.info(f"[Update Valid_from | Valid_to of Dates] : {date_pairs}")
            logger.info("--------")

            for datum in date_pairs:
                if len(datum) == 2:
                    datum_before = datum[0]
                    datum_after = datum[1]

                    logger.info(f"[Updating Timerange] : {datum}")

                    for tabs in states_tables:
                        logger.info(
                            f"[Updating Valid_from | Valid_to] : {datum} {tabs}"
                        )

                        self.cur.execute(
                            sql2.format(
                                schema=self.schema,
                                table=tabs,
                                date_vor=datum_before,
                                date_nach=datum_after,
                                vor_datum=datum_before,
                            )
                        )

                        logger.info(f"[Updated Valid_from | Valid_to] : {datum} {tabs}")
                        logger.info("..........")

                if len(datum) == 1:
                    datum_current = datum[0]
                    date_end = f"{self.tsp_valid_to}{str(datum_current)[-2:]}"  # 9999

                    for tabs in states_tables:
                        logger.info(
                            f"[Updating Current Valid_from | Valid_to] : {datum} {tabs}"
                        )

                        self.cur.execute(
                            sql2.format(
                                schema=self.schema,
                                table=tabs,
                                date_vor=datum_current,
                                date_nach=date_end,
                                vor_datum=datum_current,
                            )
                        )
                        logger.info(f"[Updated Valid_from | Valid_to] : {datum} {tabs}")
                        logger.info("..........")

        except Exception as er15:
            logger.exception(f"[Error] on Level 15 : {er15}")
            sys.exit()


def trigger_projecta_historiser():
    logger.info("Stepping into Historisation")
    projecta_historise().ignition_starter()
    logger.info("Stepping out of Historisation")
    logger.info("--------------------------")
