with user_group_messages as (
select lgd.hk_group_id,
       count(distinct(sdi.message_from)) as cnt_users_in_group_with_messages
  from STV2024080626__DWH.l_user_message as lum
  join STV2024080626__DWH.s_dialog_info as sdi
    on lum.hk_message_id = sdi.hk_message_id 
  join STV2024080626__DWH.l_groups_dialogs as lgd 
    on lum.hk_message_id = lgd.hk_message_id 
 group by lgd.hk_group_id
),
user_group_log as (
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
select ugl.hk_group_id,
       ugl.cnt_added_users,
       ugm.cnt_users_in_group_with_messages,
       round(100*(ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users),3) as group_conversion
  from user_group_log as ugl
  join user_group_messages as ugm
    on ugl.hk_group_id = ugm.hk_group_id
 order by ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users desc