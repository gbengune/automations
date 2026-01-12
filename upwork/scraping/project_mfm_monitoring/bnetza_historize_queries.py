"""
Note: Change being done here is row-based and not cell/column-based.
1.) There are id values which existed in previous datasets but dissapeared
    in current datasets(e.g 202201->202204). These ids will have change_to=NULL
    at the end of historization because it could not be determined if they
    expereinced changes or not.
2.) Only ids which existed in previous and following dates of thesame
    Bundesland will be considered for historization.
3.) Two different historization processing will take place for the last
    known dataset date. First will be done to ascertain "row change" whereby
    column "change_to" = current_date if there was change between the dataset
    rows belonging to the current date and the previous date using "id",
    thereafter actual will be set to "False" and a second historization will
    then be done whereby dataset rows of the current date which have
    values "change_from=change_to" will then get 'change_to=999999'.
4.) A left join of the current date to the previous date was chosen because
    it ensures that non-existing currentdate-ids remain actual/seen "only" in
    the current year since they get change_to=currentyear-date. The result from
    the left join, will then be filtered for just exisitng ids in the two dates
    being considered. An Inner join here would have been better, but it could
    also be that an id might occur in 2 separate rows of thesame date in the
    future due to Error so all rows will be captured if this occurs in any
    data instance. At the end those ids which during historization could
    not be checked for changes will remain change_to=NULL.
5.) The first dataset 202107 will remain change_from(202107)=change_to(202107)
"""

sql_ignition = """
----will always return 1 aggregated actual date for matching assuming new
-- dataset is already inserted in global table

with a as (
select
    max(change_from) as change_from
from
    schema.table
where
    change_from is not null
    and
    change_to is null
)
select
 b.change_from
from
 schema.table_sl b, a
where
 a.change_from = b.change_from
 and
 b.change_from is not null
 and
 b.change_to is null
 limit 1;
"""

# deriving date ranges for datasets of all German States
# For newly inserted dataset processing, always 2 date ranges/pairs will be generated with script below
# +-  and 1 date for previously current datasets in global table
sql_get_dates = """

select
    distinct change_from
from
    schema.table_sl
where
 change_to is null    -----currently freshly inserted dataset to be processed
 or
 change_to = '{default_tsp}';   ----- previous current dataset of last insert process(999999)

"""

sql_get_tables = """

select
 table_name
from
 information_schema.tables
where
 table_schema = '{schema}'  ----  'roh'
 and
 table_name~*'{tables_like}'    ----'project_mfm_'
 and
 table_name !~*'{filters}';   ---- 'bridge'
"""

# in alias changed, all changed and unchanged entities are considered

sql_historize = """
-- NEW DATES PROCESSING
with
---previous data delivery
vor as
(
select

 id,

 case when gsm_out is null then 0 else gsm_out :: integer end as gsm_out,

 case when lte is null then 0 else lte :: integer end as lte,

 case when umts is null then 0 else umts :: integer end as umts,

 case when nr is null then 0 else nr :: integer end as nr,

 case when nr_dss is null then 0 else nr_dss :: integer end as nr_dss,

 case when nr_nsa_sa is null then 0 else nr_nsa_sa :: integer end as nr_nsa_sa,

 case when nr_sa is null then 0 else nr_sa :: integer end as nr_sa


from
 {schema}.{table}
where
 change_from ='{vor_datum}'
 order by id
)
,
---actual data delivery
nach as
(
select

id,

case when gsm_out is null then 0 else gsm_out :: integer end as gsm_out,

 case when lte is null then 0 else lte :: integer end as lte,

 case when umts is null then 0 else umts :: integer end as umts,

 case when nr is null then 0 else nr :: integer end as nr,

 case when nr_dss is null then 0 else nr_dss :: integer end as nr_dss,

 case when nr_nsa_sa is null then 0 else nr_nsa_sa :: integer end as nr_nsa_sa,

 case when nr_sa is null then 0 else nr_sa :: integer end as nr_sa

from
 {schema}.{table}
where
 change_from ='{nach_datum}'
 order by id
)
,
---there are ids which were present in a previous year and abscent in the
---following year and vice versa, so full outer join was not used here.
--- At the end of the historization these ids will have change_to = NULL

mapping as (
select
 vor.id as vor_id,
 vor.gsm_out as vor_gsm_out,
 vor.lte as vor_lte,
 vor.umts as vor_umts,
 vor.nr as vor_nr,
 vor.nr_dss as vor_nr_dss,
 vor.nr_nsa_sa as vor_nr_nsa_sa,
 vor.nr_sa as vor_nr_sa,


 nach.id as  nach_id,
 nach.gsm_out as nach_gsm_out,
 nach.lte as nach_lte,
 nach.umts as nach_umts,
 nach.nr as nach_nr,
 nach.nr_dss as nach_nr_dss,
 nach.nr_nsa_sa as nach_nr_nsa_sa,
 nach.nr_sa as nach_nr_sa


from
 vor
 left join
 nach
 on
 vor.id= nach.id    ----id ist always unique in a bundesland

where
 nach.id=vor.id     -----this line will leave new ids as null

)
---Note: Run below script for ids not existing in either of the dates
---select
---        *
---    from
---        mapping
---    where
---        nach.id is null
---        or
---        vor.id is null

----Note: In update below, newly introduced ids from above left join
---       will be ignored and will stay "change_to=NULL"

update {schema}.{table} pri    -----actual table to being processed
set

actual = False
,
change_to=
case
 when    ----changed increase
 pri.id = mapping.nach_id
 and
 (
 vor_gsm_out != nach_gsm_out
 or
 vor_lte != nach_lte
 or
 vor_umts != nach_umts
 or
 vor_nr != nach_nr
 or
 vor_nr_dss != nach_nr_dss
 or
 vor_nr_nsa_sa != nach_nr_nsa_sa
 or
 vor_nr_sa != nach_nr_sa
 )

 then
 '{nach_datum}' ---newer date

-----------------
else
    '{vor_datum}' --- older date
--------------------

 end
from
 mapping
where
 mapping.nach_id=pri.id
 and
 pri.change_from='{nach_datum}'
;
"""

# After the stage above, there will be change_to=Null for previously
# non-existing ids due to non-match because their ids were previously
# non-existent

sql_update_new_ids = """
--- set timestamp for previously non-existing ids,  the nulls
update {schema}.{table}
set
    change_to ='{tsp_new_id_input}'
where
    change_from='{nach_datum}'
    and
    change_to is null;
"""

sql_update_old_actual = """
-- OLD DATES PROCESSING

---- changing column "actual" of last inserted ids ('999999'),
---- the ('888888') will remain unchanged
----- the rows which did not change from previous date will remain unchanged

update {schema}.{table} set
    actual = False,
    change_to='{vor_datum}'
where
    change_from = '{vor_datum}'
    and
    change_to= '{tsp_old_current}';   --- '999999'

"""

sql_reset_actual = """
update "{schema}"."{table}"
set
    actual= False
where
    change_from = '{vor_datum}'
    and
    change_to = '{tsp_new_id_input}';    ---'888888'

"""

# from here below, used to update newest datasets...

sql_update_current = """

update "{schema}"."{table}"
set
 actual= True,
 change_to = '{def_datum}'   ---'999999'
where
    change_from = '{current_datum}'
    and
    change_to = '{current_datum}'
;
"""

sql_update_current_new = """
---
update "{schema}"."{table}"
set
    actual= True
where
    change_from = '{current_datum}'
    and
    change_to = '{tsp_new_id_input}';  ----'888888'

"""

sql_update_current_unchanged = """

update "{schema}"."{table}"
set
 actual= True

where
    change_from = '{current_datum}'
    and
    change_to not in ('{current_datum}','{def_datum}','{tsp_new_id_input}')
;
"""

# always chooses last 2 dates and does an update on just them
valid_from_to_dates = """
with initial as (
select
distinct change_from as change_from
from
schema.table_sl
order by change_from desc
limit 2
)
select
change_from
from
initial
order by change_from asc
--- the last 2 of times to be processed when max timestamp=202404
--- 202401 & 202404 will be processed like [202401,202404],[202404]

"""

valid_from_to_dates_update = """
update "{schema}"."{table}"
set

valid_from=  TO_DATE('{date_vor}','YYYYMMDD')

,
valid_to=
case
when
'{date_nach}' ~* '9999' is  True
then
(TO_DATE('{date_nach}','YYYYMMDD') + interval '1 month - 1 day') :: date --handles leap year

else

(TO_DATE('{date_nach}','YYYYMMDD') - interval '1 day') :: date --handles leap year

END

where

change_from = '{vor_datum}'  ---last 99999 and 88888 will also be tracked

"""

sql_insert_metadata = """

SELECT
data_catalog.add_metadata(
'{schema}',-- schema_name
'{table}',-- tabellen_name
'***',--- Beschreibung
'***',-- inhaltlicher_titel
'GDI-DE',-- daten_owner
'O Aremu',-- daten_maintainer
'***',-- daten_kontakt
now () :: date,-- lieferdatum
now () :: character varying, -- datenstand
'https://start.html',-- quelle
'Webseite',-- quellenart
'Eingeschränkte Nutzung',-- lizenz
'webscraping; gdi-de',-- tags
'TECH; ENER; ENVI; GOVE',-- gruppen
'project_mfm_website_scraper',-- job_id
'airflow',-- job_art
'PROJECT'--organisation

);
"""

sql_insert_comment = """
comment on table {schema}.{table} is
'Last Updated on: {datum} by DAG: {dagid} Scraped from: {project_site}';

"""
