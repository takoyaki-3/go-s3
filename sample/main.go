package main

import (
	"fmt"
	"log"

	"github.com/takoyaki-3/go-s3"
)

func main(){
	s3,_ := gos3.NewSession("./conf.json")

	s3.UploadFromPath("main.go","test/main.go")

	var raw []byte
	s3.DownloadToRaw("test/main.go",&raw)
	fmt.Println(string(raw))

	objProps,err := s3.GetObjectList("test/")
	if err != nil {
		log.Fatalln(err)
	}
	for _,oi:=range objProps{
		fmt.Println(oi)
		if err:=s3.DeleteObject(oi.Key);err!=nil{
			log.Fatalln(err)
		}
	}
}
