package main

import (
	"fmt"

	"github.com/takoyaki-3/go-s3"
)

func main(){
	s3,_ := gos3.NewSession("./conf.json")

	s3.UploadFromPath("main.go","test/main.go")

	var raw []byte
	s3.DownloadToRaw("test/main.go",&raw)
	fmt.Println(string(raw))
}
