-- 最近7天文件统计
-- 1）删除文件
drop table if exists shard.user_dep ON CLUSTER "shard2-repl1";
drop table if exists all.user_dep ON CLUSTER "shard2-repl1";

-- 2）重新建表
-- Shard 表
CREATE TABLE shard.user_dep ON CLUSTER "shard2-repl1"
(
    `user_id` Nullable(Int64) COMMENT '用户 ID',
    `department_id` Nullable(Int64) COMMENT '部门 ID'
) ENGINE = MergeTree()
PARTITION BY right(cast(hiveHash(concat(cast(user_id as String),'-',cast(department_id as String))) as String),2)
ORDER BY right(cast(hiveHash(concat(cast(user_id as String),'-',cast(department_id as String))) as String),2);

select concat(cast(1 as String),cast(3 as String));

-- Distributed 表
CREATE TABLE IF NOT EXISTS all.user_dep ON CLUSTER "shard2-repl1"
(
    `user_id` Nullable(Int64) COMMENT '用户 ID',
    `department_id` Nullable(Int64) COMMENT '部门 ID'
) ENGINE = Distributed('shard2-repl1', 'shard', 'user_dep', rand());

-- 3）插入数据
-- 输入计算昨天日期，示例：2020-11-24
ALTER TABLE shard.user_dep ON CLUSTER 'shard2-repl1' DELETE WHERE node_id is not null;
INSERT INTO all.user_dep (user_id,department_id) VALUES ();

-- 4) 查询
select * from shard.user_dep;
select count(1) from all.user_dep; -- 677667
select count(1) from user_dep.edge_view; -- 677667

select * from user_dep.edge_view;

