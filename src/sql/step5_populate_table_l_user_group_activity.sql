INSERT INTO STV2024080626__DWH.l_user_group_activity (hk_l_user_group_activity, hk_user_id,hk_group_id,load_dt,load_src)
select distinct
       hash(hu.hk_user_id,hg.hk_group_id),
       hu.hk_user_id,
       hg.hk_group_id,
       now() as load_dt,
       's3' as load_src
 from STV2024080626__STAGING.group_log as gl
 left join STV2024080626__DWH.h_users as hu 
   on gl.user_id = hu.user_id
 left join STV2024080626__DWH.h_groups as hg 
   on gl.group_id = hg.group_id 
where 1=1
  and hash(hu.hk_user_id,hg.hk_group_id) not in (select hk_l_user_group_activity from STV2024080626__DWH.l_user_group_activity)