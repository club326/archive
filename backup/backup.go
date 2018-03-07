package main

import(
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/Sirupsen/logrus"
	"time"
	"strconv"
	// "sync"
	// "strings"
	// "errors"
)


type Backer struct {
	dsn 				string
	db 					string
	table				string
	partition			bool
	partitionDesc 		string
	partitionMethod 	string
	startTime			string
	endTime				string
	backupConcurrency 	int
	archive				Archiver
	file				Filer
	record				*Recorder
}

type Archiver struct {
	archiveDsn			string
	archiveDb			string
	archiveTable		string
}


type Filer struct {
	fileName	string
	fileDir		string
}

type Recorder struct{
	dsn			string
	db			string
	table		string
}

type Status struct {
	db 					string
	table				string
	partition_name 		string
	rows 				int64
	status				string
}
// func (bak *Backer) checkDbRole() (string,error){
// 	getRoleSQL := "show variables "
// }




func (bak *Backer) getPartitionName() ([]string,error){
	getPartitionSQL := "select PARTITION_NAME from information_schema.partitions where table_name='" + bak.table + "'" + " AND table_schema='" + bak.db + "'" +
	" AND partition_name >='" + bak.startTime + "'" + " AND partition_name<='" + bak.endTime + "'" + " AND table_rows>0 order by partition_name asc;"
	log.Println(getPartitionSQL)
	var err error
	var partitionList []string
	var partition_name string
	db, err := sql.Open("mysql", bak.dsn)
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


func ( bak *Backer) archiveData(file *Filer) error{
	partitionList, err := bak.getPartitionName()	
	if err != nil {
		log.Fatal("get partition Name error:",err)
		return err
	}
	selectAffectedMap := make(map[string] sql.Result)
	log.Println("get partitionList is: ",partitionList)
	for _,p := range partitionList {
		log.Println("trying to get partition data:",p)
		result, err := bak.selectData(p,file)
		if err != nil {
			log.Fatal("get partition data err:",err)
		}
		selectAffectedMap[p] = result
	}
	loadAffectedMap := make(map[string] sql.Result)
	for _,p := range partitionList {
		log.Println("trying to load partition data:",p)
		result, err := bak.loadData(p,file)
		if err != nil {
			log.Errorln("Error load data into archive table", err)
			return err
		}
		loadAffectedMap[p] = result
	}
	err = bak.checkAffectedRows(selectAffectedMap,loadAffectedMap)
	if err != nil {
		log.Errorln("Error in check select rows and load rows:",err)
		return err
	}
	return nil
}

//record archived data status in table
func (bak *Backer)recordStatus(s *Status) error{
	recordSQL := "insert into " + bak.record.db + "." + bak.record.table + "(archive_db,archive_table,archive_partition,total_rows,status,dateline) values (" + "'" + s.db + "','" +
	 s.table + "','" + s.partition_name + "'," + strconv.FormatInt(s.rows,10) + ",'" + s.status + "'," + strconv.FormatInt(time.Now().Unix(),10) + ");"
	 log.Println(recordSQL)
	db, err := sql.Open("mysql", bak.record.dsn)
	if err != nil {
		log.Errorln("error opening conncetion to record status database:", err)
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	// Set max lifetime for a connection.
	db.SetConnMaxLifetime(1 * time.Minute)
	_, err = db.Exec(recordSQL)
	if err != nil {
		log.Errorln("Error in insert status data:", err)
		return err
	} 
	return nil
}

//check whether select and load data is in consistency, if true record in table
func (bak *Backer)checkAffectedRows(selectMap,loadMap map[string] sql.Result) error{
	for p,result := range selectMap{
		if _,ok := loadMap[p]; ok{
			selectRows, err := result.RowsAffected()
			if err !=nil {
				log.Fatal("get select affected rows error:",err)
				return err
			}
			loadRows, err := loadMap[p].RowsAffected()
			if err !=nil {
				log.Fatal("get load affected rows error:",err)
				return err
			}
			if selectRows == loadRows{
				log.Println("partition:",p,"have equal select rows and load rows:",selectRows)
				s :=  Status{
					db : 				bak.db,
					table: 				bak.table,
					partition_name: 	p,
					rows:				loadRows,
					status:				"ok",
				}
				err := bak.recordStatus(&s)
				if err != nil {
					log.Errorln("Error in record data:",err)
					return err
				}
			} else{
				log.Println("partition:",p,"have select rows:",selectRows," and load rows:",loadRows)
				s :=  Status{
					db : 				bak.db,
					table: 				bak.table,
					partition_name: 	p,
					rows:				loadRows,
					status:				"not equal",
				}
				err := bak.recordStatus(&s)
				if err != nil {
					log.Errorln("Error in record data:",err)
					return err
				}
				return err
			}
			
		} else{
			log.Errorln("partition:",p, " not in load data")
			s :=  Status{
				db : 				bak.db,
				table: 				bak.table,
				partition_name: 	p,
				status:				"failed",
			}
			err := bak.recordStatus(&s)
			if err != nil {
				log.Errorln("Error in record data:",err)
				return err
			}
			return err
		}
	}
	return nil
}


// load data into archive table from source table
func (bak *Backer) loadData(partition string,file *Filer) (sql.Result,error){
	var err error
	db, err := sql.Open("mysql", bak.archive.archiveDsn)
	if err != nil {
		log.Errorln("Error opening connection to archive database:", err)
		return nil,err
	}
	defer db.Close()
	db.SetMaxIdleConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(1 * time.Minute)
	sql := "load data infile '" + file.fileDir + file.fileName + "_" + partition + ".txt' into table " + bak.archive.archiveDb + "." + bak.archive.archiveTable + ";"
	
	result, err := db.Exec(sql)
	if err != nil {
		log.Println("load data sql is :",sql)
		log.Errorln("Error load data :", err)
		return result, err
	}
	return result,nil
}

// get cursor data from source table
func (bak *Backer) selectData(partition string,file *Filer) (sql.Result, error){
	var err error
	// log.Println("trying to get partition data:",partition)
	db, err := sql.Open("mysql", bak.dsn)
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

	sql := "select * into outfile '" + file.fileDir + file.fileName + "_" + partition + ".txt' from " + bak.db + "." + bak.table + " partition (" + partition + ");"
	result, err := db.Exec(sql)
	if err != nil {
		log.Println("query sql is :",sql)
		log.Errorln("Error pinging mysqld:", err)
		return nil,err
	}
	//log.Printf("%v",Rows)
	
	return result,nil
}

func main() {
	
	file := Filer{
		fileDir:			"/home/mysql_bak/",
		fileName:			"lepus_mysql_status_history",
		}
	archive := Archiver{
		archiveDsn:			"dba_zs:xxx@tcp(xxx:3301)/archive",
		archiveDb:			"archive",
		archiveTable:		"mysql_status_history",
	}
	record := Recorder{
		dsn:					"dba_zs:xxx@tcp(xxx:3301)/archive",
		db:						"archive",
		table:					"archive_record_log",
	}
	bak := Backer{
		dsn:				"dba_zs:xxx@tcp(xxx:3301)/lepus",
		db:					"lepus",
		table:				"mysql_status_history",
		partition:			true,
		partitionDesc:		"ymdhi",
		partitionMethod:	"range",
		startTime:			"p20160906",
		endTime:			"p20160910",
		backupConcurrency:	1,
		archive:			archive,
		file:				file,
		record:				&record,
	}
	err := bak.archiveData(&file)
	if err != nil {
		log.Fatal("select data error:",err)
	}
}
