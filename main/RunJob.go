package main

/**
效能看板需求：https://shimo.im/docs/WYrtqDKvtdQxWWxw
后端埋点参照：https://shimo.im/sheets/vBaBIDDj3dg5JlQM/NF5AK?referer=mail_collaborator_invite&parent_id=39791541&recipient=panxianao%40shimo.im

运行参数：
	env：运行环境
		idea - 测试业务流程运行是否通畅 clickhouse、 mysql
		dev - 开发环境，测试是业务
		saas - saas 线上部署运行
	debug：是否打印明细sql (true/false)
	start\end: 起止日期，不传的话，默认是运行昨天

./RunJob -env saas -sql ./script > 2020-12-14.log 2>&1 &
go run RunJob.go -env saas -debug false --start 2020-12-01 -end 2020-12-03
go run RunJob.go -env saas -debug false
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build RunJob.go

 ck --query "insert into all.dws_enterprise_td_usage_statistic_by_global_daily FORMAT TSV" < ./2020-12-14.tsv
*/
import (
	"dashboard/config"
	"dashboard/model"
	"dashboard/utils"
	"database/sql"
	"database/sql/driver"
	"flag"
	"fmt"
	"github.com/mattn/go-sqlite3"
	"log"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"

	_ "github.com/ClickHouse/clickhouse-go"
	_ "github.com/go-sql-driver/mysql"
)

var (
	env      string
	runDebug bool
	start    time.Time
	end      time.Time

	dir string

	conf  map[string]map[string]string
	tasks []string

	memLimit int64

	writeDev bool

	// golang日期格式示例："2006-01-02 15:04:05"
	layout      = "2006-01-02"
	minusday, _ = time.ParseDuration("-24h")
	plusday, _  = time.ParseDuration("24h")

	maxRetry = 3
	sleepSec = 2 * time.Second

	fetchTree bool

	baseDir,_ = os.Getwd()
)

func getScriptDir() string {
	dir, _ := os.Getwd()
	if strings.Contains(dir, "main") {
		dir = fmt.Sprintf("%s/../", dir)
	}
	return fmt.Sprintf("%s/script", dir)
}

func loadFromYaml(name string, task string, helder interface{}) {
	path := fmt.Sprintf("%s/%s.yaml", dir, task)
	ce := utils.ConfigEngine{}
	ce.Load(path)
	ce.GetStruct(name, helder)
}

func runShell(cmd string) {
	var output []byte
	var handler *exec.Cmd
	handler = exec.Command(cmd)
	output, err := handler.Output()
	if err != nil {
		logError(err)
		os.Exit(1)
	}
	var result = strings.Trim(string(output), "\n")
	logInfo(result)
}

func runClickhouseJob(date string, task string) ([][]interface{}, error) {
	var result [][]interface{}
	connConf := conf["clickhouse"]
	cluserName := connConf["cluster-name"]

	ckScript := &model.Clickhouse{}
	loadFromYaml("Clickhouse", task, ckScript)

	truncCkSql := strings.ReplaceAll(strings.ReplaceAll(ckScript.Truncate,
		"{CLUSTER_NAME}", cluserName), "{DATE}", date)
	upsertCkSql := strings.ReplaceAll(ckScript.Upsert, "{DATE}", date)
	queryCkSql := strings.ReplaceAll(ckScript.Query, "{DATE}", date)

	dataSourceName := fmt.Sprintf("tcp://%s:%s?username=%s&password=%s",
		connConf["host"], connConf["port"], connConf["user"], connConf["pass"])

	conn, err := sql.Open("clickhouse", dataSourceName)
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(); err != nil {
		return nil, err
	}
	defer conn.Close()

	groupby_buffer_sql := fmt.Sprintf("set max_bytes_before_external_group_by=%d", memLimit)
	_, err = conn.Exec(groupby_buffer_sql)
	if err != nil {
		return nil, err
	}
	logDebug(groupby_buffer_sql)

	max_mem_sql := fmt.Sprintf("set max_memory_usage=%d", memLimit)
	_, err = conn.Exec(max_mem_sql)
	if err != nil {
		return nil, err
	}
	logDebug(max_mem_sql)

	logDebug(truncCkSql)
	_, err = conn.Exec(truncCkSql)
	if err != nil {
		return nil, err
	}

	logDebug(upsertCkSql)
	_, err = conn.Exec(upsertCkSql)
	if err != nil {
		return nil, err
	}

	// 休眠 10 秒，保证 ck 个分区数据就绪
	time.Sleep(time.Second * 10)

	logDebug(queryCkSql)
	rows, err := conn.Query(queryCkSql)
	if err != nil {
		return nil, err
	}
	result, err = parseRows(rows)
	if err != nil {
		return nil, err
	}
	logInfo("fetch %d rows", len(result))

	if len(result) > 0 {
		logDebug(result[0])
	}

	return result, nil
}

func parseRows(rows *sql.Rows) ([][]interface{}, error) {
	var result = make([][]interface{}, 0)
	cols, err := rows.Columns() // Remember to check err afterwards
	if err != nil {
		return nil, err
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	vals := make([]interface{}, len(cols))
	for i, _ := range cols {
		vals[i] = new(interface{})
	}
	for rows.Next() {
		temp := make([]interface{}, 0)
		err = rows.Scan(vals...)
		for i, e := range vals {
			if types[i].DatabaseTypeName() == "Date" {
				temp = append(temp, string([]byte(fmt.Sprintf("%s", *e.(*interface{})))[:10]))
			} else if types[i].DatabaseTypeName() == "DateTime" {
				temp = append(temp, string([]byte(fmt.Sprintf("%s", *e.(*interface{})))[:19]))
			} else if strings.Contains(types[i].DatabaseTypeName(), "String") {
				t := fmt.Sprintf("%v", *e.(*interface{}))
				t = strings.ReplaceAll(t, "\"", "\\\"")
				temp = append(temp, t)
			} else {
				t := *e.(*interface{})
				if t == nil {
					temp = append(temp, "null")
				} else {
					temp = append(temp, t)
				}
			}
		}
		result = append(result, temp)
	}
	return result, nil
}

func runMysqlJob(connConf map[string]string, date string, task string, argsArr [][]interface{}) error {
	mysqlScript := &model.Mysql{}
	loadFromYaml("Mysql", task, mysqlScript)
	truncMysqlSql := strings.ReplaceAll(mysqlScript.Truncate, "{DATE}", date)
	upsertMysqlSql := strings.ReplaceAll(mysqlScript.Upsert, "{DATE}", date)

	conn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		connConf["user"], connConf["pass"], connConf["host"], connConf["port"], connConf["db"]))
	if err != nil {
		return err
	}
	if err := conn.Ping(); err != nil {
		log.Fatal(err)
		return err
	}
	defer conn.Close()

	logInfo(truncMysqlSql)
	_, err = conn.Exec(truncMysqlSql)
	if err != nil {
		return err
	}

	logDebug(upsertMysqlSql)

	// 批量插入
	batch := 1000
	batchValues := make([]string, 0)
	temp := strings.Split(upsertMysqlSql, "(")
	template := strings.ReplaceAll("("+temp[1], "?", "\"%v\"")
	for i, args := range argsArr {
		batchValues = append(batchValues, fmt.Sprintf(template, args...))
		if (i+1)%batch == 0 || (i+1) == len(argsArr) {
			sql := temp[0] + strings.Join(batchValues, ",")
			_, err = conn.Exec(sql)
			batchValues = make([]string, 0)
			logInfo("insert job: [%d/%d]", i+1, len(argsArr))
			if err != nil {
				logError("insert to %d row", i+1)
				return err
			}
		}
	}

	return nil
}

func batchUpsert(batch int,sql string,argsArr [][]interface{},conn *sql.DB) error {
	batchValues := make([]string, 0)
	temp := strings.Split(sql, "(")
	template := strings.ReplaceAll("("+temp[1], "?", "\"%v\"")
	for i, args := range argsArr {
		batchValues = append(batchValues, fmt.Sprintf(template, args...))
		if (i+1)%batch == 0 || (i+1) == len(argsArr) {
			sql := temp[0] + strings.Join(batchValues, ",")
			_, err := conn.Exec(sql)
			batchValues = make([]string, 0)
			logInfo("insert job: [%d/%d]", i+1, len(argsArr))
			if err != nil {
				logError("insert to %d row", i+1)
				return err
			}
		}
	}
	return nil
}

func schedule() {
	if fetchTree {
		err := prepareUserDepartmentRelation()
		if err != nil{
			logError(err)
		}
	}
	//for start.Before(end) || start.Equal(end) {
	//	date := start.Format(layout)
	//	for i, task := range tasks {
	//		logInfo("[%d/%d]: run the job of '%s' at %s", i+1, len(tasks), task, date)
	//		retry := 0
	//		var rows [][]interface{}
	//		var err error
	//		for retry < maxRetry {
	//			rows, err = runClickhouseJob(date, task)
	//			if err != nil {
	//				logError("the %d time run failed, sleep for 2 seconds. %v", retry+1, err)
	//				retry += 1
	//				if retry == maxRetry {
	//					os.Exit(1)
	//				}
	//				time.Sleep(sleepSec)
	//			} else {
	//				break
	//			}
	//		}
	//
	//		retry = 0
	//		for retry < maxRetry {
	//			err = runMysqlJob(conf["mysql"], date, task, rows)
	//			if err != nil {
	//				logError("the %d time run failed, sleep for 2 seconds. %v", retry+1, err)
	//				retry += 1
	//				if retry == maxRetry {
	//					os.Exit(1)
	//				}
	//				time.Sleep(sleepSec)
	//			} else {
	//				break
	//			}
	//		}
	//
	//		// saas 写入 dev 一份
	//		if writeDev {
	//			retry = 0
	//			for retry < maxRetry {
	//				err = runMysqlJob(config.GetEnvConfig("dev")["mysql"], date, task, rows)
	//				if err != nil {
	//					logError("the %d time run failed, sleep for 2 seconds. %v", retry+1, err)
	//					retry += 1
	//					if retry == maxRetry {
	//						os.Exit(1)
	//					}
	//					time.Sleep(sleepSec)
	//				} else {
	//					break
	//				}
	//			}
	//		}
	//	}
	//
	//	logInfo("%s run success", date)
	//	start = start.Add(plusday)
	//}
}

func logDebug(v ...interface{}) {
	if runDebug {
		if len(v) == 1 {
			log.Printf("[DEBUG] %s\n", v...)
		} else {
			log.Printf("[DEBUG] %s\n", fmt.Sprintf(v[0].(string), v[1:]...))
		}
	}
}

func logInfo(v ...interface{}) {
	if len(v) == 1 {
		log.Printf("[INFO] %s\n", v...)
	} else {
		log.Printf("[INFO] %s\n", fmt.Sprintf(v[0].(string), v[1:]...))
	}
}

func logError(v ...interface{}) {
	if len(v) == 1 {
		log.Printf("[ERROR] %s\n", v...)
	} else {
		log.Printf("[ERROR] %s\n", fmt.Sprintf(v[0].(string), v[1:]...))
	}
}

func getDateObj(date string) (time.Time, error) {
	location, _ := time.LoadLocation("Local")
	//layout="2006-01-02 15:04:05"
	dateObj, err := time.ParseInLocation(layout, date, location)
	return dateObj, err
}

func getString(value driver.Value) string{
	rValue := reflect.ValueOf(value)
	inter := rValue.Interface()
	arr := inter.([]uint8)
	return string(arr)
}

func connSqlite()(*sql.DB,error){
	path := fmt.Sprintf(`%s/test.db`,baseDir)
	_, err := os.Stat(path)
	if err == nil {
		os.Remove(path)
	}
	sql.Register("sqlite3_extended",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				getDeps := func(node_id,node_type int64) (string, error) {
					isCompleted := false
					temp := make(map[string]string)
					cond := fmt.Sprintf(`%d-%d`,node_id,node_type)
					for ;!isCompleted; {
						sql := fmt.Sprintf(`select group_concat(parent_id || '-' || parent_type) as cond,group_concat(parent_id) as ids from edge 
								where (node_id || '-' || node_type) like '%s'`,cond)
						query, err := conn.Query(sql, nil)
						if err != nil {
							return "", err
						}
						cols := query.Columns()
						vals := make([]driver.Value, len(cols))
						query.Next(vals)

						if vals[0] == nil {
							isCompleted = true
						}else{
							cond = getString(vals[0])
							ids := getString(vals[1])
							if strings.Index(ids,",")==-1{
								temp[ids] = ids
							}else{
								for _,department_id := range strings.Split(ids,","){
									temp[department_id] = department_id
								}
							}
						}
					}

					ids := make([]string,0)
					for k,_ := range temp{
						ids = append(ids,fmt.Sprintf("%s",k))
					}

					return strings.Join(ids,","),nil
				}
				return conn.RegisterFunc("getDeps", getDeps, true)
			},
		})
	conn, err := sql.Open("sqlite3_extended", path)
	return conn, err
}

func runTreeSqliteJob(argsArr [][]interface{})([][]interface{},error){
	sqliteScript := &model.TreeSqlite{}
	loadFromYaml("TreeSqlite", "svc_tree", sqliteScript)

	conn, err := connSqlite()
	if err != nil{
		return nil,err
	}
	defer conn.Close()

	// 1.初始化建表
	_, err = conn.Exec(sqliteScript.CreateEdge)
	if err != nil{
		return nil,err
	}

	// 2.加载数据
	batch := 5000
	batchValues := make([]string, 0)
	temp := strings.Split(sqliteScript.Upsert, "(")
	template := strings.ReplaceAll("("+temp[1], "?", "\"%s\"")
	for i, args := range argsArr {
		batchValues = append(batchValues, fmt.Sprintf(template, args...))
		if (i+1)%batch == 0 || (i+1) == len(argsArr) {
			sql := temp[0] + strings.Join(batchValues, ",")
			_, err = conn.Exec(sql)
			if err != nil {
				logError("insert to %d row", i+1)
				return nil,err
			}
			batchValues = make([]string, 0)
			logInfo("insert job: [%d/%d]", i+1, len(argsArr))
		}
	}

	fmt.Println(time.Now())

	// 3.递推计算
	rows, err := conn.Query(sqliteScript.Call,nil)
	if err != nil {
		return nil,err
	}

	var result = make([][]interface{}, 0)
	for rows.Next() {
		var user_id int64
		var deps string
		err = rows.Scan(&user_id,&deps)
		if err != nil {
			return nil,err
		}
		for _,department_id_str := range strings.Split(deps,","){
			temp := make([]interface{}, 0)
			temp = append(temp, user_id)
			department_id, err := strconv.Atoi(department_id_str)
			if err != nil {
				return nil,err
			}
			temp = append(temp,department_id)
			result = append(result, temp)
		}
	}

	fmt.Println(time.Now())

	return result, nil
}



func runTreeMysqlJob()([][]interface{},error){
	connConf := conf["tree_mysql"]
	treeMysqlScript := &model.TreeMysql{}
	loadFromYaml("TreeMysql", "svc_tree", treeMysqlScript)

	conn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		connConf["user"], connConf["pass"], connConf["host"], connConf["port"], connConf["db"]))
	if err != nil {
		return nil,err
	}
	if err := conn.Ping(); err != nil {
		log.Fatal(err)
		return nil,err
	}
	defer conn.Close()

	logDebug(treeMysqlScript.Query)
	rows, err := conn.Query(treeMysqlScript.Query)
	if err != nil {
		return nil,err
	}

	result, err := parseRows(rows)
	if err != nil {
		return nil,err
	}
	logInfo("fetch %d rows", len(result))
	return 	result, err
}


func runTreeClickhouseJob(argsArr [][]interface{}) error{
	connConf := conf["clickhouse"]
	cluserName := connConf["cluster-name"]

	ckScript := &model.TreeClickhouse{}
	loadFromYaml("TreeClickhouse", "svc_tree", ckScript)

	dataSourceName := fmt.Sprintf("tcp://%s:%s?username=%s&password=%s",
		connConf["host"], connConf["port"], connConf["user"], connConf["pass"])

	conn, err := sql.Open("clickhouse", dataSourceName)
	if err != nil {
		return err
	}
	if err := conn.Ping(); err != nil {
		return err
	}
	defer conn.Close()

	truncateCkSql := strings.ReplaceAll(ckScript.Truncate,"{CLUSTER_NAME}",cluserName)
	logDebug(truncateCkSql)
	_, err = conn.Exec(truncateCkSql)
	if err != nil {
		return err
	}

	// 批量插入
	tx, err := conn.Begin()
	if err != nil{
		return err
	}
	stmt, err := tx.Prepare(ckScript.Upsert)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for i, args := range argsArr {
		_, err = stmt.Exec(args...)
		if err != nil {
			logError("insert to %d row", i+1)
			return err
		}
	}
	logInfo(fmt.Sprintf(`insert %d rows to clickhouse user_dep`,len(argsArr)))
	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func prepareUserDepartmentRelation() error{
	// 1.从 clickhouse 抽取 svc_tree 数据
	result, err := runTreeMysqlJob()
	if err != nil {
		return err
	}

	// 2.借助 sqlite 参与计算
	relationData, err := runTreeSqliteJob(result)
	if err != nil {
		return err
	}

	// 3.加载回 clickhouse
	err = runTreeClickhouseJob(relationData)
	if err != nil {
		return err
	}
	return nil
}

func init() {
	var runDebugStr, tasksStr, startStr, endStr, writeDevStr,fetchTreeStr string
	flag.StringVar(&env, "env", "idea", "运行环境")
	flag.StringVar(&startStr, "start", time.Now().Add(minusday).Format("2006-01-02"), "起始日期")
	flag.StringVar(&endStr, "end", time.Now().Add(minusday).Format("2006-01-02"), "结束日期")
	flag.StringVar(&tasksStr, "tasks", "all", "需要执行的任务(默认执行全部)")
	flag.StringVar(&runDebugStr, "debug", "false", "是否打印debug信息")
	flag.StringVar(&dir, "sql", getScriptDir(), "script目录")
	flag.Int64Var(&memLimit, "mem_limit", 20000000000, "clickhouse内存设置")
	flag.StringVar(&writeDevStr, "write_dev", "false", "saas运行时，是否写入dev环境")
	flag.StringVar(&fetchTreeStr, "fetch_tree", "true", "是否抓取svc_tree")

	flag.Parse()

	runDebug = runDebugStr == "true"
	fetchTree = fetchTreeStr == "true"
	logInfo("input args: env=%v, start=%v, end=%v, debug=%v, tasks=%v, write_dev=%v, fetch_tree=%v",
		env, startStr, endStr, runDebug, tasksStr, writeDevStr,fetchTreeStr)

	conf = config.GetEnvConfig(env)

	if tasksStr == "all" {
		tasks = config.GetTasks()
	} else {
		tasks = strings.Split(strings.ReplaceAll(tasksStr, " ", ""), ",")
	}

	var err error
	start, err = getDateObj(startStr)
	if err != nil {
		logError(err)
		os.Exit(1)
	}
	end, err = getDateObj(endStr)
	if err != nil {
		logError(err)
		os.Exit(1)
	}

	// saas 开启双写
	if env == "dev" {
		writeDev = false
	} else {
		writeDev = writeDevStr == "true"
	}
}

func main() {
	schedule()
}
