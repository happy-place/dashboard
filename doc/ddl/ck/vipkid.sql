-- ckexec 1.sql > 1.csv
-- 需求 1：
select user_id,user_name,count(ldate) login_dates from (
   select distinct
       ldate,
       user_id,
       user_name,
       team_id
   from shimo.events_all_view
   where ldate >= '2020-12-01'
     and ldate <= '2020-12-31'
     and team_id = '5074'
)temp group by team_id,user_id,user_name
FORMAT CSV;

-- 需求 2：
select team_id,create_docxs + create_docs as docs,create_sheets as sheets,create_dirs as dirs,create_spaces as spaces from (
   select t1.team_id,
          count(if(file_type in (2, 3), guid, null))                            as create_objs,
          count(if((file_type = 2 and sub_type in (0, -2)), guid, null))        as create_docxs,
          count(if((file_type = 2 and sub_type in (-1, -3, -4)), guid, null)) as create_sheets,
          count(if((file_type = 2 and sub_type in (-8)), guid, null))           as create_tables,
          count(if((file_type = 2 and sub_type in (-5, -10)), guid, null))      as create_ppts,
          count(if((file_type = 2 and sub_type in (-6)), guid, null))           as create_docs,
          count(if((file_type = 3), guid, null))                                as create_clouds,
          count(if((file_type = 1 and sub_type in (1)), guid, null))            as create_dirs,
          count(if((file_type = 1 and sub_type in (2)), guid, null))            as create_spaces,
          count(if((file_type = 2 and sub_type in (-7, -9)), guid, null))       as create_others
   from (
            select cast(team_id as Nullable(Int64)) as team_id,
                   cast(id as String)               as user_id,
                   toNullable(name)                 as user_name,
                   is_seat,
                   toNullable(deleted_at)           as deleted_at
            from shimo_pro.users
            where team_id = '5074'
        ) as t1
            left join
        (
            select cast(created_by as Nullable(String)) as created_by,
                   guid,
                   type as file_type,
                   sub_type
            from shimo.all_files
            where toDate(created_at + 8 * 3600) <= '2020-12-31'
        ) as t2
        on t1.user_id = t2.created_by
   group by team_id
) temp where docs + sheets + dirs + spaces >0
FORMAT CSV;

-- 需求 3：
select team_id,create_docxs + create_docs as docs,create_sheets as sheets,create_dirs as dirs,create_spaces as spaces from (
   select t1.team_id,
          count(if(file_type in (2, 3), guid, null))                            as create_objs,   -- 总新建文件数
          count(if((file_type = 2 and sub_type in (0, -2)), guid, null))        as create_docxs,  -- 新建文档(新文档)数
          count(if((file_type = 2 and sub_type in (-1, -3, -4)), guid, null)) as create_sheets, -- 新建表格数
          count(if((file_type = 2 and sub_type in (-8)), guid, null))           as create_tables, -- 新建表单数
          count(if((file_type = 2 and sub_type in (-5, -10)), guid, null))      as create_ppts,   -- 新建幻灯片数
          count(if((file_type = 2 and sub_type in (-6)), guid, null))           as create_docs,   -- 新建传统文档(专业)数
          count(if((file_type = 3), guid, null))                                as create_clouds, -- 新建云文件数
          count(if((file_type = 1 and sub_type in (1)), guid, null))            as create_dirs,   -- 新建文件夹
          count(if((file_type = 1 and sub_type in (2)), guid, null))            as create_spaces, -- 新建 团队空间
          count(if((file_type = 2 and sub_type in (-7, -9)), guid, null))       as create_others  -- 新建其他（脑图、白板，不包括空间、文件夹）
   from (
            select cast(team_id as Nullable(Int64)) as team_id,
                   cast(id as String)               as user_id,
                   toNullable(name)                 as user_name,
                   is_seat,
                   toNullable(deleted_at)           as deleted_at
            from shimo_pro.users
            where team_id = '5074'
        ) as t1
            left join
        (
            select cast(created_by as Nullable(String)) as created_by,
                   guid,
                   type as file_type,
                   sub_type
            from shimo.all_files
            where substr(cast(toDate(created_at + 8 * 3600) as String),1,7) = '2020-12'
        ) as t2
        on t1.user_id = t2.created_by
   group by team_id
) temp where docs + sheets + dirs + spaces >0
FORMAT CSV;

-- 需求 4：
select team_id,department_id,create_docxs + create_docs as docs,create_sheets as sheets,create_dirs as dirs,create_spaces as spaces from (
 select t1.team_id,t1.department_id,
        count(if(file_type in (2, 3), guid, null))                            as create_objs,   -- 总新建文件数
        count(if((file_type = 2 and sub_type in (0, -2)), guid, null))        as create_docxs,  -- 新建文档(新文档)数
        count(if((file_type = 2 and sub_type in (-1, -3, -4)), guid, null)) as create_sheets, -- 新建表格数
        count(if((file_type = 2 and sub_type in (-8)), guid, null))           as create_tables, -- 新建表单数
        count(if((file_type = 2 and sub_type in (-5, -10)), guid, null))      as create_ppts,   -- 新建幻灯片数
        count(if((file_type = 2 and sub_type in (-6)), guid, null))           as create_docs,   -- 新建传统文档(专业)数
        count(if((file_type = 3), guid, null))                                as create_clouds, -- 新建云文件数
        count(if((file_type = 1 and sub_type in (1)), guid, null))            as create_dirs,   -- 新建文件夹
        count(if((file_type = 1 and sub_type in (2)), guid, null))            as create_spaces, -- 新建 团队空间
        count(if((file_type = 2 and sub_type in (-7, -9)), guid, null))       as create_others  -- 新建其他（脑图、白板，不包括空间、文件夹）
 from (
          select team_id,tt1.user_id,user_name,is_seat,deleted_at,department_id
          from (
                   select cast(team_id as Nullable(Int64)) as team_id,
                          cast(id as String)               as user_id,
                          toNullable(name)                 as user_name,
                          is_seat,
                          toNullable(deleted_at)           as deleted_at
                   from shimo_pro.users
                   where team_id = '5074'
               ) as tt1
                       left join
                   (
                       select cast(user_id as String) as user_id,department_id
                       from shimo.organizations_view
                       where user_id is not null
                   ) as tt2 on tt1.user_id = tt2.user_id
          ) as t1
              left join
          (
              select cast(created_by as Nullable(String)) as created_by,
                     guid,
                     type as file_type,
                     sub_type
              from shimo.all_files
              where toDate(created_at + 8 * 3600) <= '2020-12-31'
          ) as t2
          on t1.user_id = t2.created_by
     group by team_id,department_id
 ) temp where docs + sheets + dirs + spaces >0
FORMAT CSV;

-- 需求 5：
select team_id,department_id,create_docxs + create_docs as docs,create_sheets as sheets,create_dirs as dirs,create_spaces as spaces from (
 select t1.team_id,department_id,
        count(if(file_type in (2, 3), guid, null))                            as create_objs,   -- 总新建文件数
        count(if((file_type = 2 and sub_type in (0, -2)), guid, null))        as create_docxs,  -- 新建文档(新文档)数
        count(if((file_type = 2 and sub_type in (-1, -3, -4)), guid, null)) as create_sheets, -- 新建表格数
        count(if((file_type = 2 and sub_type in (-8)), guid, null))           as create_tables, -- 新建表单数
        count(if((file_type = 2 and sub_type in (-5, -10)), guid, null))      as create_ppts,   -- 新建幻灯片数
        count(if((file_type = 2 and sub_type in (-6)), guid, null))           as create_docs,   -- 新建传统文档(专业)数
        count(if((file_type = 3), guid, null))                                as create_clouds, -- 新建云文件数
        count(if((file_type = 1 and sub_type in (1)), guid, null))            as create_dirs,   -- 新建文件夹
        count(if((file_type = 1 and sub_type in (2)), guid, null))            as create_spaces, -- 新建 团队空间
        count(if((file_type = 2 and sub_type in (-7, -9)), guid, null))       as create_others  -- 新建其他（脑图、白板，不包括空间、文件夹）
 from (
          select team_id,tt1.user_id,user_name,is_seat,deleted_at,department_id
          from (
                   select cast(team_id as Nullable(Int64)) as team_id,
                          cast(id as String)               as user_id,
                          toNullable(name)                 as user_name,
                          is_seat,
                          toNullable(deleted_at)           as deleted_at
                   from shimo_pro.users
                   where team_id = '5074'
               ) as tt1
                   left join
               (
                   select cast(user_id as String) as user_id,department_id
                   from shimo.organizations_view
                   where user_id is not null
               ) as tt2 on tt1.user_id = tt2.user_id
      ) as t1
          left join
      (
          select cast(created_by as Nullable(String)) as created_by,
                 guid,
                 type as file_type,
                 sub_type
          from shimo.all_files
          where substr(cast(toDate(created_at + 8 * 3600) as String),1,7) = '2020-12'
      ) as t2
      on t1.user_id = t2.created_by
 group by team_id,department_id
) temp where docs + sheets + dirs + spaces >0
FORMAT CSV;