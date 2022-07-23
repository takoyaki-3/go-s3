package gos3

import (
	"io/ioutil"
	"os"

	json "github.com/takoyaki-3/go-json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
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

func (s *Session)DownloadToRaw(objectKey string, raw *[]byte)error{

	s3Client := s3.New(s.Session)

	// Get Object
	obj, err := s3Client.GetObject(&s3.GetObjectInput{ Bucket: aws.String(s.config.BucketName), Key: aws.String(objectKey), })
	if err != nil {
		return err
	}

	resp := obj.Body
	defer resp.Close()

	b, err := ioutil.ReadAll(resp)
	if err != nil {
		return err
	}
	*raw = append([]byte{}, b...)
	return err
}

func (s *Session)UploadFromPath(targetFilePath string, objectKey string)error{
	file, err := os.Open(targetFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	uploader := s3manager.NewUploader(s.Session)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(objectKey),
		Body:   file,
	})
	return err
}
