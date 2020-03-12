package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/cute-angelia/go-utils/cache/bunt"
	"github.com/cute-angelia/go-utils/file"
	"github.com/guonaihong/gout"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mssql"
	ants "github.com/panjf2000/ants/v2"
	"github.com/spf13/viper"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	db *gorm.DB
	wg sync.WaitGroup

	f  *os.File
	f2 *os.File
	f3 *os.File

	client *oss.Client
	bucket *oss.Bucket

	err error
)

const Tag = "deleted"
const TagValue = "true"

func HandleError(err error) {
	fmt.Println("Error:", err)
	os.Exit(-1)
}

func init() {
	// 缓存
	bunt.InitBuntCache("cache", "gameavat.db")

	// 初始化-db
	db, _ = gorm.Open("mssql", "sqlserver://sa:9065163@192.168.2.110:1433?database=test")

	viper.SetConfigName("config") // name of config file (without extension)
	viper.SetConfigType("json") // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath("../../config")   // path to look for the config file in
	viper.AddConfigPath("config")  // call multiple times to add many search paths

	err := viper.ReadInConfig() // Find and read the config file
	if err != nil { // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	} else {
		// log.Print(viper.Get("yq"))
	}

	// 创建OSSClient实例。
	client, err = oss.New(viper.Get("yq.endpoint").(string), viper.Get("yq.accessKeyId").(string),  viper.Get("yq.accessKeySecret").(string))
	if err != nil {
		HandleError(err)
	}

	// 获取存储空间。
	bucket, err = client.Bucket(viper.Get("yq.bucketName").(string))
	if err != nil {
		HandleError(err)
	}
}

func main() {

	var prefixf = flag.String("prefix", "FACE/205", "前缀Path, 如： FACE/205")
	var markerf = flag.String("marker", "", "起始路径，如 FACE/205/xxxxx.jpg")
	flag.Parse()

	if len(*prefixf) == 0 {
		flag.Usage()
		return
	}

	fileprefix := strings.Replace(*prefixf, "/", "_", -1)

	txtPath := fmt.Sprintf("/tmp/go_game_touxiang_need_deleted_%s.txt", fileprefix)
	f = file.OpenLocalFileWithFlagPerm(txtPath, os.O_RDWR|os.O_APPEND, 0666)

	txtPath2 := fmt.Sprintf("/tmp/go_game_touxiang_no_deleted_%s.txt", fileprefix)
	f2 = file.OpenLocalFileWithFlagPerm(txtPath2, os.O_RDWR|os.O_APPEND, 0666)

	// go_game_touxiang_no_exits_1583900368.txt
	txtPath3 := fmt.Sprintf("/tmp/go_game_touxiang_no_exits_%s.txt", fileprefix)
	f3 = file.OpenLocalFileWithFlagPerm(txtPath3, os.O_RDWR|os.O_APPEND, 0666)

	defer db.Close()
	defer f.Close()
	defer f2.Close()
	defer f3.Close()

	uri := fmt.Sprintf("https://sc.ftqq.com/SCU9426Tf8c93224ef853531d39171ed2ee44dda594b812d41eff.send?text=%s&desp=%s", "删除头像脚本通知", "数据处理完成" + fileprefix)
	defer func() {
		gout.GET(uri).Do()
		log.Println("数据处理完成", fileprefix, txtPath)
	}()

	// 列举所有文件。
	// marker := "FACE/205/"
	marker := oss.Marker(*markerf)
	prefix := oss.Prefix(*prefixf)

	p, _ := ants.NewPool(1)
	defer p.Release()

	for {
		// lsRes, err := bucket.ListObjects(oss.Marker(marker))

		// 列举包含指定前缀的文件。默认列举100个文件。
		lsRes, err := bucket.ListObjects(oss.MaxKeys(1000), marker, prefix)

		if err != nil {
			HandleError(err)
		}

		// 打印列举文件，默认情况下一次返回100条记录。
		for _, object := range lsRes.Objects {
			wg.Add(1)

			p.Submit(func() {
				doSomethings(object)
				wg.Done()
			})
			//if err := doSomethings(object); err != nil {
			//	continue
			//}
		}

		if lsRes.IsTruncated {
			prefix = oss.Prefix(lsRes.Prefix)
			marker = oss.Marker(lsRes.NextMarker)
		} else {
			break
		}
	}

	wg.Wait()
	p.Running()
}

func doSomethings(object oss.ObjectProperties) error {
	// jpg
	keys := strings.Split(object.Key, "_")
	if len(keys) > 0 && strings.Contains(object.Key, ".jpg") && strings.Contains(object.Key, "FACE") {
		// fmt.Println("\nBucket: ", object.Key, object.LastModified)

		k := keys[0]

		userids := strings.Split(k, "/")

		userid := userids[len(userids)-1]

		// 查询 userid 数据

		faceurl := ""
		err := errors.New("")
		if faceurl, err = bunt.Get("cache", userid); err != nil || len(faceurl) == 0 {
			userinfo := struct {
				FaceUrl string `gorm:"column:faceurl"`
			}{}
			z := db.New()
			if err := z.Table("user_info").Select("faceurl").Where("userid = ?", userid).Order("userid desc").First(&userinfo).Error; err != nil {
				log.Println("数据查询失败")
			} else {
				bunt.Set("cache", userid, userinfo.FaceUrl, time.Hour*100010)
			}

			if len(userinfo.FaceUrl) == 0 {
				log.Println("数据不存在", userid)
				f3.WriteString(object.Key + "\n")
				return fmt.Errorf("数据不存在")
			} else {
				faceurl = userinfo.FaceUrl
			}
		}

		// log.Println("faceurl",faceurl)

		if z, err := url.Parse(faceurl); err != nil {
			return fmt.Errorf("链接不正确")
		} else {
			//if exits, err := bunt.Get("cache", k); err != nil {
			//	log.Println("err", err)
			//} else {
			// 读取数据
			//if len(exits) > 0 {
			// 处理时间
			//log.Println("value", exits)

			// 获取object的Tagging信息。
			//taggingResult, err := bucket.GetObjectTagging(object.Key)
			//if err != nil {
			//	fmt.Println("Error:", err)
			//	os.Exit(-1)
			//}

			// fmt.Printf("Object Tagging: %v\n", taggingResult)

			//for _,v := range taggingResult.Tags {
			//	if v.Key == Tag && v.Value == TagValue {
			//		// log.Println("发现tag:" +  Tag + " > 该条记录需要删除")
			//	}
			//}

			// 判断时间， 存入最新时间
			timestamps := object.LastModified.Unix()
			//timestamps_cache, _ := strconv.Atoi(exits)

			//小于导出数据时间不处理数据
			timez, _ := time.Parse("2006-01-02 15:04:05", "2020-03-10 00:00:00")

			if timestamps < timez.Unix() {
				// log.Println("时间", int(timestamps) , timestamps_cache, int(timestamps) >= timestamps_cache)

				//if int(timestamps) >= timestamps_cache {
				//	bunt.Set("cache", k, strconv.Itoa(int(timestamps)), time.Hour*1209)
				//} else {
				// 走删除逻辑
				a1 := fmt.Sprintf("/%s", object.Key)
				if strings.Compare(a1, z.Path) == 0 {
					log.Println("不删除， 数据库一致", object.Key)

					f2.WriteString(object.Key + "\n")

				} else {
					log.Println("删除 --------> ", object.Key, z.Path)
					setTag(object.Key, bucket)

					f.WriteString(object.Key + "\n")

					// todo
					// 图片时间大于 当前时间， 不能删除
					// deleteObject(object.Key, bucket)
				}
				//}
			} else {
				log.Println("不处理", object.Key, timez, object.LastModified)
			}

			//} else {
			// 存入
			//	timestamps := object.LastModified.Unix()
			//	bunt.Set("cache", k, strconv.Itoa(int(timestamps)), time.Hour*1209)
			//}
			//}
		}
	}
	return nil
}

func setTag(objkey string, bucket *oss.Bucket) {
	// 设置Tagging规则。
	tag1 := oss.Tag{
		Key:   Tag,
		Value: TagValue,
	}
	tagging := oss.Tagging{
		Tags: []oss.Tag{tag1},
	}
	// 设置Object Tagging。
	err := bucket.PutObjectTagging(objkey, tagging)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(-1)
	}
}

func deleteObject(objkey string, bucket *oss.Bucket) {
	// 删除单个文件。objectName表示删除OSS文件时需要指定包含文件后缀在内的完整路径，例如abc/efg/123.jpg。
	// 如需删除文件夹，请将objectName设置为对应的文件夹名称。如果文件夹非空，则需要将文件夹下的所有object删除后才能删除该文件夹。
	err := bucket.DeleteObject(objkey)
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(-1)
	}
}
