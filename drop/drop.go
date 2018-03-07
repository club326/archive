package main

import(
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/Sirupsen/logrus"
	"time"
	// "sync"
	// "strings"
	"errors"
)


type Droper struct {
	masterDSN 			string
	db 					string
	table				string
	partition			bool
	partitionDesc 		string
	partitionMethod 	string
	startTime			string
	endTime				string
	record				*Recorder
}

type Recorder struct{
	dsn			string
	db			string
	table		string
}



//get partition List going to drop
func (drop *Droper) getPartitionName() ([]string,error){
	getPartitionSQL := "select PARTITION_NAME from information_schema.partitions where table_name='" + drop.table + "'" + " AND table_schema='" + drop.db + "'" +
	" AND partition_name >='" + drop.startTime + "'" + " AND partition_name<='" + drop.endTime + "'" + " AND table_rows>0 order by partition_name asc;"
	var err error
	var partitionList []string
	var partition_name string
	db, err := sql.Open("mysql", drop.masterDSN)
	if err != nil {
		log.Errorln("error opening conncetion to database:", err)
		return partitionList, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	// Set max lifetime for a connection.
	db.SetConnMaxLifetime(1 * time.Minute)
	rows, err := db.Query(getPartitionSQL)
	if err != nil {
		log.Errorln("Error get partition list:", err)
		return partitionList, err
	}
	for rows.Next(){
		err := rows.Scan(&partition_name)
		if err != nil {
			log.Fatal("get partition_name failed:",err)
			return partitionList, err
		}
		log.Println("raw partition_name:",partition_name)
		partitionList =append(partitionList,partition_name)

	}
	
	return partitionList,nil
}

// trying to drop partition
func (drop *Droper) dropData(partition string) (sql.Result, error){
	var err error
	// log.Println("trying to get partition data:",partition)
	db, err := sql.Open("mysql", drop.masterDSN)
	if err != nil {
		log.Errorln("Error opening connection to database:", err)
		return nil,err
	}
	defer db.Close()
	// By design exporter should use maximum one connection per request.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	// Set max lifetime for a connection.
	db.SetConnMaxLifetime(1 * time.Minute)

	sql := "alter table " + drop.db + "." + drop.table + " drop partition " + partition + ";"
	result, err := db.Exec(sql)
	if err != nil {
		log.Println("trying to drop partition:",sql)
		log.Errorln("Error in drop partition:", err)
		return nil,err
	}
	//log.Printf("%v",Rows)
	
	return result,nil
}

//check drop partitions whether or not archived
func (drop *Droper) checkArchiveData(p string ) (int,string,int,error){
	querySQL := "select total_rows,status,dateline from " + drop.record.db + "." + drop.record.table + " where archive_db = '" + drop.db + "' and archive_table = '" + drop.table + "'" +
	 " and archive_partition = '" + p + "';"
	log.Println(querySQL)
	var total_rows 		int
	var status 			string
	var dateline 		int
	db, err := sql.Open("mysql", drop.record.dsn)
	if err != nil {
	   log.Errorln("error opening conncetion to record status database:", err)
	   return total_rows,status,dateline, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	// Set max lifetime for a connection.
	db.SetConnMaxLifetime(1 * time.Minute)
	rows, err := db.Query(querySQL)
	if err != nil {
	   log.Errorln("Error in query archive status data:", err)
	   return total_rows,status,dateline,err
	}
	for rows.Next(){
		err := rows.Scan(&total_rows,&status,&dateline)
		if err != nil {
			log.Errorln("Error in scan query archive rows:",err)
			return total_rows,status,dateline,err
		}
	}
	return total_rows,status,dateline,nil
}


func (drop *Droper) Execute() error {
	partitionList, err := drop.getPartitionName()
	if err != nil {
		log.Errorln("Error in drop partition:",err)
		return err
	}
	for _,partition := range partitionList {
		total_rows,status,dateline,err := drop.checkArchiveData(partition)
		if err != nil {
			log.Errorln("get partition archive status error:",partition,",total_rows:",total_rows,",status:",status,",dateline:",dateline,",error:",err)
			return err
		}
		if status != "ok" {
			err := errors.New("Error in get partition status")
			log.Errorln("get partition:",partition," status:",status)
			return err
		}
		result, err := drop.dropData(partition)	
		if err != nil {
			log.Errorln("Error in drop partition data:",err)
			return err
		}
		//TODO: can update archive status in table in further
		rows , err := result.RowsAffected(); if err != nil {
			log.Errorln("drop partition failed and affected rows:",rows)
			return err
		}
		log.Println("drop partition:",partition,",affected rows:",rows)
	}
	return nil
}

func main() {
	
	record := Recorder{
		dsn:					"dba_zs:zhaoshan123*@tcp(192.168.11.103:3301)/archive",
		db:						"archive",
		table:					"archive_record_log",
	}
	drop := Droper{
		masterDSN:			"dba_zs:zhaoshan123*@tcp(192.168.11.103:3301)/lepus",
		db:					"lepus",
		table:				"mysql_status_history",
		partition:			true,
		partitionDesc:		"ymdhi",
		partitionMethod:	"range",
		startTime:			"p20160906",
		endTime:			"p20160910",
		record:				&record,
	}
	
	err := drop.Execute()
	if err != nil {
		log.Fatal("select data error:",err)
	}
}
