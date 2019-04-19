package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ada-wang/mylogging"
	"github.com/samuel/go-zookeeper/zk"
)

////////////////////////////////////////
// for mylogging
////////////////////////////////////////
const (
	pkgLogID = "main"
	level    = mylogging.DEBUG
)

var logger = mylogging.MustGetLogger(pkgLogID)

func init() {
	mylogging.SetModuleLevel(level, pkgLogID)
}

////////////////////////////////////////
// for mylogging - end
////////////////////////////////////////

// GetArgs get os.args
func GetArgs() []string {
	if len(os.Args) != 3 {
		logger.Error("arguments should be 2")
		os.Exit(1)
	}
	ip := os.Args[1]
	port := os.Args[2]

	srv := ip + ":" + port
	srvList := make([]string, 0)
	srvList = append(srvList, srv)
	logger.Debug(srvList)
	return srvList
}

func main() {
	conn, _, err := zk.Connect(GetArgs(), time.Second) //*10)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	detailFile, err := os.OpenFile("detail.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		logger.Error(err)
		os.Exit(1)
	}
	defer detailFile.Close()

	zookeeperWatcherFile, err := os.OpenFile("zookeeperWatcher.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		logger.Error(err)
		os.Exit(1)
	}
	defer zookeeperWatcherFile.Close()

	for {
		children, stat, ch, err := conn.ChildrenW("/brokers/ids")
		if err != nil {
			panic(err)
		}
		fmt.Printf("%+v %+v\n", children, stat)
		if len(children) >= 4 {
			zookeeperWatcherFile.WriteString("INFO " + strings.Join(children, " ") + "\n")
		} else if len(children) == 3 {
			zookeeperWatcherFile.WriteString("WARNING " + strings.Join(children, " ") + "\n")
		} else {
			zookeeperWatcherFile.WriteString("ERROR " + strings.Join(children, " ") + "\n")
		}
		
		zookeeperWatcherFile.Sync()
		detailFile.WriteString(time.Now().String() + " kafka brokers: " + strings.Join(children, " ") + "\n")
		detailFile.Sync()
		e := <-ch
		fmt.Printf("%+v\n", e)
	}

}
