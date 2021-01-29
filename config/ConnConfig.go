package config

import (
	"fmt"
)

var (
	config = map[string]map[string]map[string]string{
		"dev": { // env
			"clickhouse": { // source
				"host":         "localhost",
				"port":         "9000",
				"user":         "chadmin",
				"pass":         "1vF3tO3EK2Av4",
				"db":           "all",
				"cluster-name": "shard2-repl1",
			},
			"mysql": {
				"host": "localhost",
				"port": "3306",
				"user": "bigdata",
				"pass": "7b2Nu6JFEtgH4",
				"db":   "boss",
			},
			"tree_mysql": {
				"host": "localhost",
				"port": "3306",
				"user": "root",
				"pass": "root",
				"db":   "svc_tree",
			},
		},
	}
)

func GetEnvConfig(env string) map[string]map[string]string {
	result, ok := config[env]
	if !ok {
		panic(fmt.Sprintf("unknow env %s", env))
	}
	return result
}

func GetConnConfig(env string, source string) map[string]string {
	return config[env][source]
}
