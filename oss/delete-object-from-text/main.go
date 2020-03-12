package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/cute-angelia/go-example/common"
	"github.com/spf13/viper"
	"log"
	"os"
)

var (
	client *oss.Client
	bucket *oss.Bucket
)

func init() {
	viper.SetConfigName("config")       // name of config file (without extension)
	viper.SetConfigType("json")         // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath("../../config") // path to look for the config file in
	viper.AddConfigPath("config")       // call multiple times to add many search paths

	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	} else {
		// log.Print(viper.Get("yq"))
	}

	// 创建OSSClient实例。
	client, err = oss.New(viper.Get("yq.endpoint").(string), viper.Get("yq.accessKeyId").(string), viper.Get("yq.accessKeySecret").(string))
	if err != nil {
		common.PanicError("创建OSSClient实例", err)
	}

	// 获取存储空间。
	bucket, err = client.Bucket(viper.Get("yq.bucketName").(string))
	if err != nil {
		common.PanicError("获取存储空间", err)
	}
}

func main() {
	inputText := flag.String("text", "", "input text")

	if len(*inputText) == 0 {
		flag.Usage()
		return
	}

	file, err := os.Open(*inputText)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lineText := scanner.Text()
		deleteObject(lineText, bucket)
	}
}

func deleteObject(objkey string, bucket *oss.Bucket) {
	// 删除单个文件。objectName表示删除OSS文件时需要指定包含文件后缀在内的完整路径，例如abc/efg/123.jpg。
	// 如需删除文件夹，请将objectName设置为对应的文件夹名称。如果文件夹非空，则需要将文件夹下的所有object删除后才能删除该文件夹。
	err := bucket.DeleteObject(objkey)
	if err != nil {
		common.LogError("删除对象失败", err)
	}
}
