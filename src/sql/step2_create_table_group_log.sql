drop table if exists STV2024080626__STAGING.group_log;

create table STV2024080626__STAGING.group_log
(
    id identity(1,1) primary key,
    group_id  int REFERENCES STV2024080626__STAGING.groups(id),
    user_id   int REFERENCES STV2024080626__STAGING.users(id),
    user_id_from int REFERENCES STV2024080626__STAGING.users(id),
    event  varchar(10),
    datetime timestamp(6)
)
order by group_id, user_id
PARTITION BY datetime::date
GROUP BY calendar_hierarchy_day(datetime::date, 3, 2);