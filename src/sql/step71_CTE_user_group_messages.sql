with user_group_messages as (
select lgd.hk_group_id,
       count(distinct(sdi.message_from)) as cnt_users_in_group_with_messages
  from STV2024080626__DWH.l_user_message as lum
  join STV2024080626__DWH.s_dialog_info as sdi
    on lum.hk_message_id = sdi.hk_message_id 
  join STV2024080626__DWH.l_groups_dialogs as lgd 
    on lum.hk_message_id = lgd.hk_message_id 
 group by lgd.hk_group_id
)
select hk_group_id,
       cnt_users_in_group_with_messages
  from user_group_messages
 order by cnt_users_in_group_with_messages
 limit 10