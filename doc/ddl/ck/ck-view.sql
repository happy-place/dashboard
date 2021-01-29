drop view if exists shimo.events_all_view on cluster "shard2-repl1";
create view shimo.events_all_view on cluster "shard2-repl1" as select t1.*,t2.team_id,t2.user_name,t2.is_seat from (
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
    select cast(team_id as Nullable(Int64)) as team_id,cast(id as String) as user_id,toNullable(name) as user_name,is_seat,toNullable(deleted_at) as deleted_at from shimo_dev.users
) t2 on t1.user_id = t2. user_id;

select * from shimo.events_all_view;

drop view if exists shimo.organizations_view on cluster "shard2-repl1";
create view shimo.organizations_view on cluster "shard2-repl1" as select team_id, t1.department_id, user_id from
(
    select toNullable(parent_id) as team_id, toNullable(node_id) as department_id
    from svc_tree.edge
    where parent_type = 9
      and node_type = 10
) t1
    full outer join
(
    select toNullable(parent_id) as department_id, node_id as user_id
    from svc_tree.edge
    where parent_type = 10
      and node_type = 11
    group by user_id
) t2 on t1.department_id = t2.department_id;

select t1.node_id as department_id,t2.node_id as child_department_id from
(select parent_id,node_id from svc_tree.edge where parent_type = 9  and node_type = 10) as t1
left join
(select parent_id,node_id from svc_tree.edge where parent_type = 9  and node_type = 10) as t2
on t1.node_id = t2.parent_id;

select toNullable(parent_id) as department_id, node_id as user_id
from svc_tree.edge
where parent_type = 10 and node_type = 11 and department_id in (297,296,275,274,260);

select * from svc_tree.edge where parent_type = 10 and node_type = 11 and node_id in (13203,13204);

select toUnixTimestamp(toDateTime('2020-12-12 00:00:00'))  as c1;

drop table if exists shimo.all_files ON CLUSTER "shard2-repl1";
create view shimo.all_files on cluster 'shard2-repl1' as
select *,'file' as source from svc_file.file
union all
select *,'file_legacy' as source from svc_file.file_legacy;







