package gos3

import (
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	json "github.com/takoyaki-3/go-json"
)

type Config struct {
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	Region string `json:"region"`
	BucketName string `json:"bucket_name"`
	EndPoint string `json:"endpoint"`
}

type Session struct {
	// 
	Session *session.Session
	config Config
}

func NewSession(configFilePath string)(s Session, err error){
	if err:=json.LoadFromPath(configFilePath,&s.config);err!=nil{
		return s,err
	}
	s.Session = session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(s.config.Region),
		Credentials: credentials.NewStaticCredentials(
			s.config.AccessKey, s.config.SecretKey, "",
		),
		Endpoint: aws.String(s.config.EndPoint),
	}))
	return s,nil
}

func (s *Session)DownloadToReaderFunc(objectKey string, f func(w io.Reader)error)error{
	s3Client := s3.New(s.Session)

	// Get Object
	obj, err := s3Client.GetObject(&s3.GetObjectInput{ Bucket: aws.String(s.config.BucketName), Key: aws.String(objectKey), })
	if err != nil {
		return err
	}

	resp := obj.Body
	defer resp.Close()

	return f(resp)
}

func (s *Session)DownloadToRaw(objectKey string, raw *[]byte)error{
	return s.DownloadToReaderFunc(objectKey,func(r io.Reader) error {
		b, err := ioutil.ReadAll(r)
		if err != nil {
			return err
		}
		*raw = append([]byte{}, b...)
		return err	
	})
}

func (s *Session)UploadFromReader(r io.Reader, objectKey string)error{
	uploader := s3manager.NewUploader(s.Session)
	uploader.PartSize = 5 * 1024 * 1024 * 1024
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(objectKey),
		Body:   r,
	})
	return err
}

func (s *Session)UploadFromPath(targetFilePath string, objectKey string)error{
	file, err := os.Open(targetFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	return s.UploadFromReader(file,objectKey)
}

type ObjectProperty struct {
	Size int64
	Key string
	LastModified string
}

func (s *Session)GetObjectList(prefix string)(objProps []ObjectProperty,err error){

	s3Client := s3.New(s.Session)

	params := &s3.ListObjectsV2Input{Bucket: &s.config.BucketName, Prefix: &prefix}
	jst, _ := time.LoadLocation("Asia/Tokyo")

	// wg := sync.WaitGroup{}
	// wg.Add(1)

	s3Client.ListObjectsV2Pages(params,
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			// defer wg.Done()
			for _, obj := range page.Contents {
				objProps = append(objProps, ObjectProperty{
					Size: *obj.Size,
					Key: *obj.Key,
					LastModified: obj.LastModified.In(jst).Format("2006-01-02 15:04:05"),
				})
			}
			return *page.IsTruncated
		})

	// wg.Wait()
	return objProps,err
}

func (s *Session)DeleteObject(key string)(err error){
	
	s3Client := s3.New(s.Session)

	_, err = s3Client.DeleteObject(&s3.DeleteObjectInput{Bucket: &s.config.BucketName, Key: &key})
	if err != nil {
		return err
	}

	return s3Client.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: &s.config.BucketName,
		Key:    &key,
	})
}
