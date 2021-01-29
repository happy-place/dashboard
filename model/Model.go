package model

type Clickhouse struct {
	Truncate string
	Upsert   string
	Query    string
}

type Mysql struct {
	Truncate string
	Upsert   string
}

type TreeSqlite struct {
	CreateEdge string
	Upsert string
	Call string
}

type TreeClickhouse struct {
	Truncate string
	Upsert   string
}

type TreeMysql struct {
	Query    string
}

