sql_get_laender = """
---tab(0),tab(1),tab(2)
select
    kennung,
    bundesland,
    data_format,
    last_upload
from
    "{schema}"."{table}"
    order by kennung asc;
"""

sql_cr_tabs = """
drop table if exists "{schema}"."{table}" ;
create table "{schema}"."{table}" ({columns});

comment on table "{schema}"."{table}" is 'Data Source is: {zip_file}';

"""

sql_table_entity = """
select
    table_schema,
    table_name,
    string_agg(concat('"',column_name,'"'),',') as cols
from
    information_schema.columns
where
    table_schema='{schema}'
 and
    table_name ='{table}'
 and
    column_name not in ('geom','nr_gesamt','nr_alle')
    group by
    table_schema,table_name
"""

sql_insert_in_global = """

insert into "{schema}"."{table}"({columns},actual,change_from,partitioned)
select
    {columns},   -----concatinated columns here
    {stand},    ----if actual or not actual
    {data_date},  ---the date from online content
    {partition_id} ----the bundesland kennung from bridge table

from
    {schema}."{s_table}";

"""

sql_delete_table = """

drop table if exists "{schema}"."{table}";
"""


sql_update_datum = """
update
 "{schema}"."{table}"
set
 last_upload='{datum}'
 where bundesland = '{german_state}';
"""
