package main

import(
	"github.com/julienschmidt/httprouter"
	"net/http"
	log "github.com/Sirupsen/logrus"
	"fmt"
	"strings"
)

type handler struct{
	router 	*httprouter.Router
	// apiV1	
}

//http://localhost:8888/backup/instance=192.168.11.103:3301&db=lepus&table=archive
func Backup(w http.ResponseWriter, r *http.Request, p httprouter.Params){
	target := strings.Split(p.ByName("archiveTarget"),"&")
	for _,tg := range target {
		fmt.Fprintln(w,tg)
	}
	if r.Method == "POST" {
		fmt.Fprintln(w,"test")
	}
	
}

func main(){
	router := httprouter.New()
	router.GET("/backup/:archiveTarget",Backup)
	log.Fatal(http.ListenAndServe(":8888", router))
	router.POST("/backup/:archiveTarget",Backup)
	
}