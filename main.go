package main

import (
	"sync"

	"github.com/Kong/go-pdk"
	"github.com/Kong/go-pdk/server"

	"github.com/gocql/gocql"
	// "github.com/scylladb/gocql"
)

type Config struct {
	ScyllaDBAddress   string `json:"scyllaDBAddress"`
	KeySpaceName      string `json:"keySpaceName"`
	ReplicationFactor string `json:"replicationFactor"`
	Stategy           string `json:"stategy"`
	TableName         string `json:"tableName"`
}

func New() interface{} {
	return &Config{}
}

var (
	session *gocql.Session
	once    sync.Once
)

func (c *Config) createKeyspace(kong *pdk.PDK) bool {
	q := "CREATE KEYSPACE IF NOT EXISTS " + c.KeySpaceName + " WITH replication = { 'class': '" + c.Stategy +
		"' , 'replication_factor': " + c.ReplicationFactor + "};"
	if err := session.Query(q).Exec(); err != nil {
		kong.Log.Err("failed to execute query", err)
		return false
	}
	return true

}

func (c *Config) createTable(kong *pdk.PDK) bool {
	q := "CREATE TABLE IF NOT EXISTS" + c.KeySpaceName + "." + c.TableName + " (vin text PRIMARY KEY, host text);"
	if err := session.Query(q).Exec(); err != nil {
		kong.Log.Err("failed to execute query", err)
		return false
	}
	return true
}

func (c *Config) createMatiriaizedView(kong *pdk.PDK) bool {
	q := "CREATE MATERIALIZED VIEW IF NOT EXISTS " + c.KeySpaceName + "." + c.TableName +
		"_by_host AS SELECT host, vin FROM " + c.KeySpaceName + "." + c.TableName +
		" WHERE host IS NOT NULL AND vin IS NOT NULL PRIMARY KEY (host, vin);"

	if err := session.Query(q).Exec(); err != nil {
		kong.Log.Err("failed to execute query", err)
		return false
	}

	return true
}

func (c *Config) initSession(kong *pdk.PDK) bool {
	cluster := gocql.NewCluster(c.ScyllaDBAddress)

	var err error
	session, err = cluster.CreateSession()
	if err != nil {
		kong.Log.Err("failed to connect ScyllaDB", err)
		return false
	}

	res := c.createKeyspace(kong)
	if res == false {
		return res
	}

	res = c.createTable(kong)
	if res == false {
		return res
	}

	res = c.createMatiriaizedView(kong)

	return res
}

func (c *Config) Accsess(kong *pdk.PDK) {
	once.Do(func() {
		res := c.initSession(kong)
		if res == false {
			s := "failed to initialize database session" 
			kong.Log.Err(s)
			byteSlice := []byte(s)
			kong.Response.Exit(500, byteSlice, nil)
	
		}
	})
	// url, err := kong.Request.GetUrl()
	// if err != nil {
	// 	kong.Log.Err("failed to get url", err)
	// 	return

	// }

	vin, err := kong.Request.GetHeader("vin")
	if err != nil {
		kong.Log.Err("failed to get VIN from headers", err)
		s := "failed finding VIN" + vin + "in request headers"
		byteSlice := []byte(s)
		kong.Response.Exit(400, byteSlice, nil)
		return
	}

	host, err := c.queryScylla(vin)
	if err != nil {
		kong.Log.Err("failed to find host for VIN", err)
		s := "failed finding host for VIN " + vin
		byteSlice := []byte(s)
		kong.Response.Exit(404, byteSlice, nil)

		return
	}
	if host == "NA" {
		s := "Vin not found"
		byteSlice := []byte(s)
		kong.Response.Exit(404, byteSlice, nil)
		kong.Log.Info("VIN is now in a NOT connected state", err)
		return

	}

	kong.ServiceRequest.SetPath(host)
}

func (c *Config) queryScylla(vin string) (string, error) {
	var host string
	q := "SELECT host FROM " + c.KeySpaceName + "." + c.TableName + " WHERE vin = ? LIMIT 1;"

	if err := session.Query(q, vin).Scan(&host); err != nil {
		return "", err
	}
	return host, nil

}

func main() {
	server.StartServer(New, "0.1", 1000)
}
