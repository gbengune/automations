"""
Details:
    O.Aremu
    9.Oct.2025

Pipeline:
    Checks new projecttitle upload information from server2 and
    imports them into server3

Note:
    server2 - op-postgres prod
    server3 - server3 postgres prod

    server2 - Changes are tracked ***independent***
    - of the queue_all(history) table
    server3 - Changes are tracked based on 3
    - (history) columns (fod_id, version, upload).

    If in a cell in queue server3 the entry
    - (unbekannt) is seen in the upload column,
    means the projecttitle no longer exists in server2.

FYI:
    This is a rough draft without Scheduling DAG involvements
    Exception handling levels were all with info, because the code
    - was meant for a prod env.
"""

import psycopg2 as psg
import pandas as pd
import datetime
import logging


logger = logging.getLogger(__name__)
time = datetime.datetime.now()


class update_queue_table:
    def __init__(self):

        # dag id
        self.comment = "Table Updated by Dag"
        self.dag_id = "SERVER3_UPDATE_QUEUE"
        self.date = datetime.datetime.now()

        # Table properties
        self.server2_projecttitle_schema = "***"
        self.server2_table_names = """***"""

        self.server2_snapshot_schema = "***"
        self.server2_snapshot_table = "***"

        self.server3_queue_schema = "***"
        self.server3_table_name = "***"

        # Table attributes

        self.server3_columns = """***"""
        self.changed_columns = "***"

        # string replacements
        self.nan = "None"

        # lines variable
        self.seg = "----------"

        # connection server2_op
        self.cred_server2 = {
            "pg_host": "***",
            "pg_user": "***",
            "pg_port": None,
            "pg_pass": "***",
            "pg_sdb": "***",
        }

        self.conn_server2 = psg.connect(
            host=self.cred_server2["pg_host"],
            user=self.cred_server2["pg_user"],
            port=self.cred_server2["pg_port"],
            password=self.cred_server2["pg_pass"],
            database=self.cred_server2["pg_sdb"],
        )
        self.cur_server2 = self.conn_server2.cursor()

        # connection server3_op
        self.cred_server3 = {
            "pg_host": "***",
            "pg_user": "***",
            "pg_port": None,
            "pg_pass": "***",
            "pg_sdb": "***",
        }

        self.conn_server3 = psg.connect(
            host=self.cred_server3["pg_host"],
            user=self.cred_server3["pg_user"],
            port=self.cred_server3["pg_port"],
            password=self.cred_server3["pg_pass"],
            database=self.cred_server3["pg_sdb"],
        )
        self.conn_server3.autocommit = True
        self.cur_server3 = self.conn_server3.cursor()

    def main(self):

        self.rebuild_queue(
            cur=self.cur_server3,
            sql=sql_build_queue,
            schema=self.server3_queue_schema,
            table=self.server3_table_name,
        )

        server2_queue = self.get_projecttitle_details(
            db="server2",
            cur=self.cur_server2,
            sql=sql_get_queue_server2,
            schema=self.server2_projecttitle_schema,
            table=self.server2_table_names,
            ext_schema=self.server2_snapshot_schema,
            ext_table=self.server2_snapshot_table,
            ext_column=None,
        )

        server3_queue = self.get_projecttitle_details(
            db="server3",
            cur=self.cur_server3,
            sql=sql_get_queue_server3,
            schema=self.server3_queue_schema,
            table=self.server3_table_name,
            ext_schema=None,
            ext_table=None,
            ext_column=self.server3_columns,
        )

        # allocating same columns to server2 & 3 datasets
        df_cols = self.to_list(column_string=self.server3_columns)
        # logger.info(df_cols)

        if server2_queue is not None and server3_queue is not None:

            # converting db-query results to dataframes
            dt_server2 = self.details_to_dataframe(
                entry=server2_queue, db_columns=df_cols
            )
            dt_server3 = self.details_to_dataframe(
                entry=server3_queue, db_columns=df_cols
            )

            # # extracting non duplicate dataframe rows as new
            # projecttitle uploads (aggregated)

            server2_new_uploads = self.extract_new_uploads(dt_server2,
                                                           dt_server3)

            new_uploads = self.get_uploads_from_df_series(
                df_new_upload_series=server2_new_uploads
            )

            self.insert_new_uploads(
                cur=self.cur_server3,
                sql=sql_insert_queue_server3,
                schema=self.server3_queue_schema,
                table=self.server3_table_name,
                column=self.server3_columns,
                latest_uploads=new_uploads,
            )

        else:
            pass

        self.close_conns()

    def get_projecttitle_details(
        self, db, cur, sql, schema, table, ext_schema, ext_table, ext_column
    ) -> list[tuple]:
        """
        Method reads data from database server
        :param db: database server name
        :param cur: cursor
        :param sql: sql code
        :param schema: postgres schema in database
        :param table: postgres table in database
        :param ext_schema: external schema for snapshots
        :param ext_table: external table in database for snapshots
        :param ext_column: external column in database for snapshots
        :return: method return values
        """
        if db == "server2":

            try:

                logger.info(f"[Processing Dataset Snapshot] : {db}")

                server2_tables = self.to_list(column_string=table)

                cur.execute(
                    sql.format(
                        columns=ext_column,
                        schema=schema,
                        table_np=server2_tables[0],
                        table_ads=server2_tables[1],
                        table_mitt=server2_tables[2],
                        table_vn=server2_tables[3],
                        table_summary=server2_tables[4],
                        snap_schema=ext_schema,
                        snap_table=ext_table,
                    )
                )

                bfp_data = cur.fetchall()

                logger.info(f"[Processed Dataset Snapshot] : {db}")
                logger.info(self.seg)

                return bfp_data

            except Exception as er1:
                logger.info(f"[Error Encountered Checking server2 Registry] : "
                            f"{er1}")

        if db == "server3":

            try:

                logger.info(f"[Processing Dataset Snapshot] : {db}")

                cur.execute(sql.format(columns=ext_column,
                                       schema=schema,
                                       table=table))

                bfp_data = cur.fetchall()

                logger.info(f"[Processed Dataset Snapshot] : {db}")
                logger.info(self.seg)

                return bfp_data

            except Exception as er2:
                logger.info(f"[Error Encountered Checking server3 Registry] : "
                            f"{er2}")

    def details_to_dataframe(self, entry: list[tuple], db_columns):
        """
        Method converts data to pandas dataframe. Normally belongs in utils.py
        :param entry: dataset for pandas dataframing
        :param db_columns: columns
        :return: method return
        """
        try:

            df = pd.DataFrame(
                data=entry, index=range(0, len(entry)), columns=db_columns
            )
            return df

        except Exception as er3:
            logger.info(f"[Error Encountered in dataframe creation] : {er3}")

    def to_list(self, column_string: str) -> list:
        """
        Method converts data to list
        :param column_string: takes string columns and converts to list
        :return: method return list
        """
        list_keys = column_string.split(",")
        return list_keys

    def extract_new_uploads(self, *dtfs):
        """
        Method extracts new uploads from
        - dtfs by comparing old and new datasets in dataframes
        :param dtfs: dataframe args
        :return: returns difference rows
        """
        try:

            df_server2 = dtfs[0]
            df_server3 = dtfs[1]

            # selecting columns to be used for checking change
            col = self.to_list(column_string=self.changed_columns)

            # extracting server2 row aggs which are not present in server3
            new_uploads = df_server2[
                (
                    ~df_server2[col[0]].astype(str).isin(df_server3[col[0]].astype(str))
                    & ~df_server2[col[1]]
                    .astype(str)
                    .isin(df_server3[col[1]].astype(str))
                    & ~df_server2[col[2]]
                    .astype(str)
                    .isin(df_server3[col[2]].astype(str))
                )
            ]

            # distinct duplicate rows using columns (col)
            filter_duplicates = new_uploads.drop_duplicates(subset=col)
            return filter_duplicates

        except Exception as er4:
            logger.info(f"[Error Encountered Filtering New Uploads] : {er4}")

    def get_uploads_from_df_series(self, df_new_upload_series) -> list:
        """
        Method cleans datasets
        :param df_new_upload_series: new changed dataset rows
        :return:
        """
        im_container = []

        # check if new changed rows exist
        if df_new_upload_series.empty is False:

            try:

                # replacing pandas (None) will postgresql (NULL)
                remove_df_nan = df_new_upload_series.fillna(self.nan)

                for ind, h in remove_df_nan.iterrows():

                    # preview
                    # tuples = (h.iloc[0], h.iloc[1], h.iloc[2])

                    tuples = tuple([h.iloc[i] for i in range(0, len(h))])

                    im_container.append(tuples)

                # logger.info(im_container)
                return im_container

            except Exception as er5:
                logger.info(f"[Error Encountered in uploaded row series] : "
                            f"{er5}")

        else:
            logger.info("[Status] : No new Upload(s) found")
            logger.info(self.seg)

            pass

    def insert_new_uploads(
        self, cur, sql, schema, table, column, latest_uploads: list
    ) -> None:
        """
        Method inserts changed and new rows from server
         - 2 into server 3 history table
        :param cur: cursor
        :param sql: sql code
        :param schema: database schema
        :param table: database table
        :param column: database columns
        :param latest_uploads: changed and cleaned dataset in tuple rows
        :return:
        """
        # inserts each row, bulk insert unadvantageous for future debugging
        if latest_uploads:

            try:

                logger.info(f"[Table Updating] : {table}")

                for rows in latest_uploads:
                    cur.execute(
                        sql.format(
                            schema=schema,
                            table=table,
                            columns=column,
                            features=rows
                        )
                    )
                logger.info(f"[Table Updated] : {table}")
                logger.info(self.seg)

                full_comment = (f"{self.comment}: {self.dag_id}"
                                f"on Date: {self.date}")
                self.insert_comment(
                    cur=cur,
                    sql=sql_comment_queue_server3,
                    schema=schema,
                    table=table,
                    pre_comment=full_comment,
                )

            except Exception as er6:
                logger.info(f"[Error Encountered] : {er6}")

    def insert_comment(self, cur, sql, schema, table, pre_comment) -> None:
        """
        Method inserts new scheduling comments in server 3 history table
        :param cur: cursor
        :param sql: sql code
        :param schema: database schema
        :param table: database table
        :param pre_comment: comment string of Airflow DAG name
        :return: None
        """
        cur.execute(sql.format(schema=schema,
                               table=table,
                               comment=pre_comment))

        logger.info(f"[Table Commented] : {table}")
        logger.info(self.seg)

    def rebuild_queue(self, cur, sql, schema, table) -> None:
        """
        Method rebuilds server 3 history table in case it magically dissapears
        :param cur: cursor
        :param sql: sql code
        :param schema: database schema
        :param table: database table
        :return:  None
        """

        logger.info(f"[Checking Queue for Rebuild] : {table}")
        cur.execute(sql.format(schema=schema, table=table))

        logger.info(f"[Checked Queue for Rebuild] : {table}")
        logger.info(self.seg)

    def close_conns(self):
        """
        Method closes server 2 and  3  connections
        after all processes have finished running.
        :return:
        """
        self.conn_server2.close()
        self.conn_server3.close()


sql_build_queue = """

create table if not exists {schema}.{table}
(
gid serial
,fod_id integer
,vs                 varchar(30)
,eakte              varchar(30)
,antragsteller      varchar(30)
,uploaded           varchar(30)
,eingereicht        varchar(30)
,added_queue        varchar(30)
,foerdergegenstand  varchar(30)
,status             varchar(30)
,call               varchar(30)
,phase              varchar(30)
,vdsl               varchar(30)
,fttc               varchar(30)
,ftth               varchar(30)
,fttb               varchar(30)
,lyrcount integer
,regionalschluessel varchar(30)
,geonode timestamp
,server3_pruef_prod  timestamp
,imported_at timestamp
,import_error varchar(30)
)
"""


sql_get_queue_server2 = """


WITH
alle_netzplaene AS (
SELECT
properties -> 'fod_id'::text AS fid,
properties -> 'version'::text AS vs,
max("timestamp") AS "timestamp",
count(DISTINCT properties -> 'layer'::text) AS lyrcount
FROM
{schema}.{table_np}
where

properties -> 'version' not in ('531','631','931')
and
timestamp >= now() - interval '1 months'


GROUP BY
1,2


UNION ALL

---Anfang Grauflecken-Adressen Seite
select
fod_id::text as fid,
np_version as vs,
max(created_at) as "timestamp",
1 as lyrcount

from
{schema}.{table_ads}



group by
fod_id,np_version


UNION ALL

---Anfang Mittelanforderung Seite
SELECT
properties -> 'fod_id'::text AS fid,
properties -> 'version'::text AS vs,
max("timestamp") AS "timestamp",
count(DISTINCT properties -> 'layer'::text) AS lyrcount
FROM
{schema}.{table_mitt}
WHERE
---(properties -> 'eingereicht'::text = 'true'::text)
timestamp >= now() - interval '1 months'

GROUP BY

1,2
---order by properties -> 'fod_id' asc


UNION ALL

---Anfang Verwendungsnachweise Seite
SELECT
properties -> 'fod_id'::text AS fid,
properties -> 'version'::text AS vs,
max("timestamp") AS "timestamp",
count(DISTINCT properties -> 'layer'::text) AS lyrcount
FROM
{schema}.{table_vn}
where
---(properties -> 'eingereicht'::text = 'true'::text)
timestamp >= now() - interval '1 months'

GROUP BY
1,2
)
,

sort_current as (
select
fod_id
,aktuelle_projecttitleversion
,antragsteller
,rs_antragsteller
,eakte
,foerdergegenstand
,status
,eingereicht
,technology
,phase
,call

,row_number() over (partition by fod_id order by fid desc)  as ranked

from

{snap_schema}.{snap_table}
)
,


---sorting last np upload to get value of last projecttitle "status"
--- using projecttitle_snapshot to get pre_existing antrag_snapshot
---used to source for the "mittelanforderung"
----and "zwischennachweise" not captured in adb_stats_tables

snapshots as (

select
alle_netzplaene.fid :: integer as fod_id,
alle_netzplaene.vs as aktuelle_projecttitleversion,

---sort_current.fod_id, ---rem
---sort_current.aktuelle_projecttitleversion,  ---rem

sort_current.antragsteller,
sort_current.rs_antragsteller,
sort_current.eakte, sort_current.foerdergegenstand,
sort_current.status,sort_current.eingereicht,
sort_current.technology,

CASE
WHEN
left(substring(alle_netzplaene.vs, 2, 1),2) = '0'
THEN '1 Antragstellung'

WHEN
left(substring(alle_netzplaene.vs, 2, 1),2) = '1'
THEN '2 Planung'

WHEN
left(substring(alle_netzplaene.vs, 2, 1),2) = '2'
THEN '3 Zwischennachweis'

WHEN
left(substring(alle_netzplaene.vs, 2, 1),2) = '3'
THEN '4 Verwendungsnachweis'

WHEN
length(alle_netzplaene.vs) = 1
THEN '1 Antragstellung'

END as phase,

sort_current.call,

alle_netzplaene.lyrcount,
to_char(alle_netzplaene."timestamp", 'DD.MM.YYYY, HH24:MI:SS' ) as uploaded



from alle_netzplaene left join sort_current
on
alle_netzplaene.fid :: text = sort_current.fod_id :: text
and
sort_current.ranked=1

)

select

fod_id
,aktuelle_projecttitleversion
,antragsteller
,rs_antragsteller
,eakte
,foerdergegenstand
,status
,eingereicht


,CASE
WHEN
lower(technology) ~~ '%vdsl%'
THEN 'VDSL'

WHEN lower(technology) ~~ '%vectoring%' THEN 'VDSL'
END as vdsl

,

CASE
WHEN
lower(technology) ~~ '%fttc%'
THEN 'FTTC'
END as fttc

,case
---- for grauflecken when technology is unknown
when technology is null and fod_id >= 1000000 then  'FTTH'

---for weisseflecken gewerbe and infra when technology is unknown
when technology is null and fod_id < 1000000 then  'FTTH/B'

WHEN lower(technology) ~~ '%ftth%' THEN 'FTTH'
END as ftth

,
CASE
WHEN
lower(technology) ~~ '%fttb%'
THEN 'FTTB'
END as fttb

,phase
,case
when call is null then '0.unbekannt'
when call = 'NULL' then '0.unbekannt'
else call
end as call

,case

when lyrcount  is null then  1
else
lyrcount

end as lyrcount

,case
when uploaded is null then 'unbekannt'
else
uploaded
end as uploaded



from
snapshots
where

--- removing old gewerbe antraege with fod_id < 500000
--- removing deleted netzplaene from OP

uploaded is not null;


"""

sql_get_queue_server3 = """


select

fod_id
,vs
,antragsteller
,regionalschluessel
,eakte
,foerdergegenstand
,status
,eingereicht

,CASE
WHEN
lower(vdsl) ~~ '%vdsl%'
THEN 'VDSL'

WHEN lower(vdsl) ~~ '%vectoring%' THEN 'VDSL'
END as vdsl


,CASE
WHEN
lower(fttc) ~~ '%fttc%'
THEN 'FTTC'
END as fttc

,case
---- for grauflecken when technology is unknown
when ftth is null and fod_id >= 1000000 then  'FTTH'

---for weisseflecken gewerbe and infra when technology is unknown
when ftth is null and fod_id < 1000000 then  'FTTH/B'

WHEN lower(ftth) ~~ '%ftth%' THEN 'FTTH'
END as ftth

,

CASE
WHEN
lower(fttb) ~~ '%fttb%'
THEN 'FTTB'
END as fttb

,phase
,case
when call is null then '0.unbekannt'
when call = 'NULL' then '0.unbekannt'
else call
end as call

,case

when lyrcount  is null then  1
else
lyrcount

end as lyrcount

,case
when uploaded is null then 'unbekannt'
else
uploaded
end as uploaded


from
    {schema}.{table} ;

"""

sql_insert_queue_server3 = """
insert into {schema}.{table}
({columns})
values {features};

"""

sql_comment_queue_server3 = """
comment on table {schema}.{table} is
'{comment}';
"""

if __name__ == "__main__":
    logger.info(f"[Process Start] : {time}")
    update_queue_table().main()
    update_queue_table().close_conns()
    logger.info(f"[Process End] : {datetime.datetime.now()}")
