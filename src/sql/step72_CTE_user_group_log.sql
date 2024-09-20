with user_group_log as (
select t.hk_group_id,
       t.cnt_added_users
  from (select luga.hk_group_id, 
               count(distinct(hk_user_id)) as cnt_added_users
          from STV2024080626__DWH.s_auth_history as sah
          join STV2024080626__DWH.l_user_group_activity as luga
            on sah.hk_l_user_group_activity = luga.hk_l_user_group_activity 
           and sah.event = 'add'
         group by luga.hk_group_id) t 
  join STV2024080626__DWH.h_groups hg 
    on t.hk_group_id = hg.hk_group_id 
 order by hg.registration_dt asc 
 limit 10
) 
select hk_group_id,
       cnt_added_users
  from user_group_log 
 order by cnt_added_users
 limit 10