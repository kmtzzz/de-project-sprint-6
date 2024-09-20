drop table if exists STV2024080626__DWH.s_auth_history;

create table STV2024080626__DWH.s_auth_history
(
  hk_l_user_group_activity bigint not null CONSTRAINT fk_s_auth_history_l_user_group_activity REFERENCES STV2024080626__DWH.l_user_group_activity (hk_l_user_group_activity),
  user_id_from int,
  event varchar(10),
  event_dt timestamp(6),
  load_dt datetime,
  load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
;

INSERT INTO STV2024080626__DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event, event_dt, load_dt,load_src)
select luga.hk_l_user_group_activity,
       gl.user_id_from,
       gl.event,
       gl.datetime,
       now() as load_dt,
       's3' as load_src
from STV2024080626__STAGING.group_log as gl
left join STV2024080626__DWH.h_users as hu
  on gl.user_id = hu.user_id
left join STV2024080626__DWH.h_groups as hg
  on gl.group_id = hg.group_id 
left join STV2024080626__DWH.l_user_group_activity as luga
  on luga.hk_user_id = hu.hk_user_id 
 and luga.hk_group_id = hg.hk_group_id 