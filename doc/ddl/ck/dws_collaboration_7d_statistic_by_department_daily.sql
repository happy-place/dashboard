-- 最近7天文件统计
-- 1）删除文件
drop table if exists shard.dws_collaboration_7d_statistic_by_department_daily ON CLUSTER "shard2-repl1";
drop table if exists all.dws_collaboration_7d_statistic_by_department_daily ON CLUSTER "shard2-repl1";

-- 2）重新建表
-- Shard 表
CREATE TABLE shard.dws_collaboration_7d_statistic_by_department_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `department_id` Nullable(String) COMMENT '部门ID',
    `add_collaborations` Nullable(Int64) COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `use_ats` Nullable(Int64) COMMENT '使用@次数：包括评论中的@ ',
    `public_shares` Nullable(Int64) COMMENT '公开分享',
    `comments` Nullable(Int64) COMMENT '评论次数',
    `file_views` Nullable(Int64) COMMENT '浏览文件数',
    `create_files` Nullable(Int64) COMMENT '创建文件数'
) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(ldate)
    ORDER BY ldate;

-- Distributed 表
CREATE TABLE IF NOT EXISTS all.dws_collaboration_7d_statistic_by_department_daily ON CLUSTER "shard2-repl1"
(
    `ldate` Date COMMENT '日期：最近7天最后一天',
    `team_id` Nullable(String) COMMENT '企业ID',
    `department_id` Nullable(String) COMMENT '部门ID',
    `add_collaborations` Nullable(Int64) COMMENT '添加协作次数（按添加协作操作的动作次数算）',
    `use_ats` Nullable(Int64) COMMENT '使用@次数：包括评论中的@ ',
    `public_shares` Nullable(Int64) COMMENT '公开分享',
    `comments` Nullable(Int64) COMMENT '评论次数',
    `file_views` Nullable(Int64) COMMENT '浏览文件数',
    `create_files` Nullable(Int64) COMMENT '创建文件数'
) ENGINE = Distributed('shard2-repl1', 'shard', 'dws_collaboration_7d_statistic_by_department_daily', rand());

-- 3）插入数据
-- 输入计算昨天日期，示例：2020-11-24
ALTER TABLE shard.dws_collaboration_7d_statistic_by_department_daily ON CLUSTER 'shard2-repl1' DELETE WHERE ldate = '2020-11-26';
INSERT INTO all.dws_collaboration_7d_statistic_by_department_daily (ldate,team_id,department_id,add_collaborations,use_ats,public_shares,comments,file_views,create_files)
SELECT
    '2020-11-26' as theDate, -- 截止日期，02号计算，落在01号,注意不能跟MergeTree表的分区字段重名，否则会出现不可预知问题
    team_id, -- 企业id
    department_id, -- 部门名称
    count(if(action_name='add_collaborator', 1,null)) as add_collaborations,
    count(if(action_name='at', 1,null)) as use_ats,
    count(if(action_name='public_share' and status = '1', 1,null)) as public_shares,
    count(if(action_name='comment', 1,null)) as comments,
    count(if(action_name='view_file', 1,null)) as file_views,
    count(if(action_name = 'create_obj' AND file_type != 1, 1,null)) as create_files
FROM
    (
        SELECT
            ldate,
            team_id,
            action_name,
            cast(user_id as Int64) as user_id,
            visitParamExtractRaw(extend_info,'status') as status,
            file_type
        FROM shimo.events_all_view
        WHERE ldate >= addDays(toDate('2020-11-26'), -6) AND ldate <= '2020-11-26' -- 最近七天(截止昨天，即输入日期)
          AND file_type != 0 -- 0 unknown 脏数据
          AND (
                (action_name='add_collaborator')
                OR (action_name='at')
                OR (action_name='public_share' AND visitParamExtractRaw(extend_info,'status') = '1')
                OR (action_name='comment')
                OR (action_name='view_file')
                OR (action_name = 'create_obj' AND file_type != 1)
            )
        AND team_id is not null
    ) T1
        INNER JOIN
    (
        select user_id,department_id from shimo.organizations_view where user_id is not null
    ) T2 on T1.user_id=T2.user_id
GROUP BY team_id,department_id;

-- 4) 查询
select * from shard.dws_collaboration_7d_statistic_by_department_daily where ldate = '2020-11-26';
select * from all.dws_collaboration_7d_statistic_by_department_daily where ldate = '2020-11-26';


