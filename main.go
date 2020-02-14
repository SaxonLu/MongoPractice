package main

import (
	"fmt"
	"log"
	"time"

	"github.com/spf13/viper"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// --------------

const (
	_day = -7
)

const _cMsgAt = "message_at"

var (
	serverName = "msgat-script"
)

func main() {
	fmt.Println("script init ...")
	initConfig()

	log.Println("gennMsgAtCollection -----任務開始-----")
	err := initMgoConn(viper.GetString("mongo.url"), viper.GetString("mongo.db_msg"))
	if err != nil {
		log.Fatal("Run", err, "mongoDB conn fail")
		return
	}
	log.Println("gennMsgAtCollection")
	beforeT := time.Now().AddDate(0, 0, _day)
	atDatas, err := findMsgsATByRoomID(beforeT)
	if err != nil {
		log.Fatal("取得atData出現問題")
	}
	fmt.Println("取得資料 共", len(atDatas))
	iData := setInsertData(atDatas)

	if err != nil {
		log.Fatal("setInsertData 出現問題")
	}

	fmt.Println("資料準備完成 共", len(iData))

	err = insertIntoMsgAt(iData)
	if err != nil {
		log.Fatal("insertIntoMsgAt 出現問題")
	}

	log.Println("gennMsgAtCollection -----任務結束-----")
}

//----------------------

const _cRoom = "rooms"

// Room :
type Room struct {
	ChatroomID string    `bson:"_id" mapstructure:"_id"`
	Type       string    `bson:"t" mapstructure:"t"`
	Msgs       int64     `bson:"msgs" mapstructure:"msgs"`
	CreatedAt  time.Time `bson:"ts" mapstructure:"ts"`
	UT         time.Time `bson:"ut" mapstructure:"ut"`
}

const (
	_cMessage = "messages"
	_cMember  = "members"
)

// Message :
type Message struct {
	ID         bson.ObjectId `bson:"_id,omitempty" mapstructure:"_id"`
	ChatroomID string        `bson:"cid" mapstructure:"cid"`
	MsgID      int64         `bson:"msg_id" mapstructure:"msg_id"`
	Sender     string        `bson:"sender" mapstructure:"sender"`
	Name       string        `bson:"name" mapstructure:"name"`
	MsgType    string        `bson:"msg_type" mapstructure:"msg_type"`
	CreateAt   time.Time     `bson:"ts" mapstructure:"ts"`
	Message    []string      `bson:"msg" mapstructure:"msg"`
	DelBy      string        `bson:"delBy" mapstructure:"delBy"`
	RefG       string        `bson:"refG,omitempty" mapstructure:"refG"`
	ExpireAt   time.Time     `bson:"expireAt" mapstructure:"expireAt"`
	DeletedAt  time.Time     `bson:"deletedAt" mapstructure:"deletedAt"`
	AtUids     []string      `bson:"at_uids" mapstructure:"at_uids"`
}

type Member struct {
	ID         bson.ObjectId `bson:"_id,omitempty" mapstructure:"_id"`
	ChatroomID string        `bson:"cid" mapstructure:"cid"`
	UserID     string        `bson:"uid" mapstructure:"uid"`
}

type MsgAt struct {
	ID         bson.ObjectId `bson:"_id,omitempty" mapstructure:"_id"`
	ChatroomID string        `bson:"cid" mapstructure:"cid"`
	MsgID      int64         `bson:"msg_id" mapstructure:"msg_id"`
	AtID       string        `bson:"at_id,omitempty" mapstructure:"at_id"`
	CreatedAt  time.Time     `bson:"ts" mapstructure:"ts"`
}

// findMsgs : in - k :cid , v : msg_id
func findMsgs(in map[string]int64) (result []Message, err error) {
	or := []bson.M{}
	for k, v := range in {
		condition := bson.M{"cid": k, "msg_id": v}
		or = append(or, condition)
	}
	query := bson.M{"$or": or}

	conn := _session.Copy()
	defer conn.Close()
	c := conn.DB(_mgMSGdb).C(_cMessage)
	err = c.Find(query).All(&result)

	return
}

func findMsgsATByRoomID(beforeT time.Time) (result []Message, err error) {
	cdata := []Message{}
	conn := _session.Copy()
	defer conn.Close()
	query := bson.M{"at_uids": bson.M{"$ne": "null"}, "ts": bson.M{"$gte": beforeT}}

	c := conn.DB(_mgMSGdb).C(_cMessage)
	err = c.Find(query).All(&cdata)

	for _, v := range cdata {
		if len(v.AtUids) > 0 {
			result = append(result, v)
		}
	}
	return
}

func setInsertData(req []Message) (result []MsgAt) {
	for _, v := range req {
		data := MsgAt{
			ChatroomID: v.ChatroomID,
			MsgID:      v.MsgID,
			CreatedAt:  v.CreateAt,
		}
		for _, atid := range v.AtUids {
			data.AtID = atid
			result = append(result, data)
		}
	}

	return
}

func insertIntoMsgAt(req []MsgAt) (err error) {
	conn := _session.Copy()
	defer conn.Close()

	c := conn.DB(_mgMSGdb).C(_cMsgAt)
	var obfile []interface{}
	for _, v := range req {
		obfile = append(obfile, v)
	}

	err = c.Insert(obfile...)
	if err != nil {
		return
	}

	return
}

// ----------------------------------------------------------------
// init
func initConfig() {
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		log.Fatal("config init error", err)
	}
}

// mongo base
var _session *mgo.Session
var _mgMSGdb string

func initMgoConn(mongoURL, db string) (err error) {
	conn, err := mgo.Dial(mongoURL)
	if err != nil {
		return err
	}
	conn.DB(db)
	conn.SetMode(mgo.Monotonic, true)
	conn.SetPoolLimit(100)

	_mgMSGdb = db
	_session = conn
	return nil
}

// ----------------------------------------------------------------
