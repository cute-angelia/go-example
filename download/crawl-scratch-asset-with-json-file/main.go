package main

import (
	"flag"
	"github.com/tidwall/gjson"
	"github.com/cute-angelia/go-utils/file"
	"log"
	"fmt"
	"strings"
	"path/filepath"
	"os"
)

const UrlFormat = "https://cdn.assets.scratch.mit.edu/internalapi/asset/%s/get"

func main() {
	jsonfile := flag.String("json", "", "input json file path")
	dir := flag.String("dir", "", "input json file dir path")
	path := flag.String("path", "", "input save path")
	flag.Parse()

	if len(*path) == 0 {
		flag.Usage()
		return
	}

	log.Println(*jsonfile)

	fileList := getFilelist(*dir)

	for _, f := range fileList {
		if jsondata, err := file.GetFileWithLocal(f); err != nil {
			log.Panic(err)
		} else {
			// log.Println(string(jsondata))
			gjson.Parse(string(jsondata)).ForEach(func(key, value gjson.Result) bool {
				uri := generateUrl(value.Get("md5").String())
				// log.Println(uri)
				downloader(uri, *path)
				return true
			})
		}
	}
}

func getFilelist(searchDir string) []string {
	fileList := []string{}
	filepath.Walk(searchDir, func(path string, f os.FileInfo, err error) error {
		if !f.IsDir() {
			fileList = append(fileList, path)
		}
		return nil
	})

	return fileList
}

func generateUrl(key string) string {
	return fmt.Sprintf(UrlFormat, key)
}

func downloader(uri string, path string) error {
	tempUrl := strings.Split(uri, "/")
	z := path + "/" + tempUrl[len(tempUrl)-2]
	return file.DownloadFileWithSrc(uri, z)
}
