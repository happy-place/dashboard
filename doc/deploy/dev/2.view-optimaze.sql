-- 功能：创建视图修复部分数据问题。

-- shimo.events_all 中 team_id 埋点上报问题(都是0 或 null)，只能从 shimo_dev.users中去team_id
drop view if exists shimo.events_all_view on cluster "shard2-repl1";
create view shimo.events_all_view on cluster "shard2-repl1" as
select t1.*,t2.team_id,is_seat,user_name,deleted_at from (
    select ldate,
        event_type,
        guid,
        user_id,
        device_id,
        file_type,
        sub_type,
        time,
        action_name,
        action_param,
        user_agent,
        extend_info
    from shimo.events_all
) t1
left join (
    select team_id,cast(id as String) as user_id,name as user_name,is_seat,deleted_at from shimo_dev.users
) t2 on t1.user_id = t2. user_id;

-- 树形组织架构展成二维表方便使用
drop view if exists shimo.organizations_view on cluster "shard2-repl1";
create view shimo.organizations_view on cluster "shard2-repl1" as
select team_id, t1.department_id, user_id from
    (
        select parent_id as team_id, node_id as department_id
        from svc_tree.edge
        where parent_type = 9
          and node_type = 10
    ) t1
    full outer join
(
    select parent_id as department_id, node_id as user_id
    from svc_tree.edge
    where parent_type = 10
      and node_type = 11
) t2 on t1.department_id = t2.department_id;

-- 文件两个表合二为一
drop table if exists shimo.all_files ON CLUSTER "shard2-repl1";
create view shimo.all_files on cluster 'shard2-repl1' as
select *,'file' as source from svc_file.file
union all
select *,'file_legacy' as source from svc_file.file_legacy;
