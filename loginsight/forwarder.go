package loginsight

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/cloudfoundry-community/firehose-to-syslog/logging"
)

type Forwarder struct {
	LogInsightReservedFields []string
	url                      *string
	hasJSONLogMsg            bool
	token					 *string
	debug                    bool
	channel                  chan *ChannelMessage
}

//NewForwarder - Creates new instance of LogInsight that implments logging.Logging interface
func NewForwarder(logInsightServer string, logInsightPort int, logInsightServerToken string, logInsightReservedFields string,
 logInsightHasJsonLogMsg, debugging bool, concurrentWorkers int, insecureSkipVerify bool) logging.Logging {

	url := fmt.Sprintf("https://%s:%d/le-mans/v1/streams/ingestion-pipeline-stream", logInsightServer, logInsightPort)
	logging.LogStd(fmt.Sprintf("Using %s for log insight", url), true)
	token := fmt.Sprintf( "Bearer %s",logInsightServerToken)
	http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: insecureSkipVerify}

	theForwarder := &Forwarder{
		LogInsightReservedFields: strings.Split(logInsightReservedFields, ","),
		url:                      &url,
		hasJSONLogMsg:            logInsightHasJsonLogMsg,
		debug:                    debugging,
		token: 					  &token,
		channel:                  make(chan *ChannelMessage, 1024),
	}
	for i := 0; i < concurrentWorkers; i++ {
		go theForwarder.ConsumeMessages()
	}

	return theForwarder
}

func (f *Forwarder) Connect() bool {
	return true
}

func (f *Forwarder) CreateKey(k string) string {
	if contains(f.LogInsightReservedFields, k) {
		return "cf_" + k
	} else {
		return k
	}
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func (f *Forwarder) ShipEvents(eventFields map[string]interface{}, msg string) {
	channelMessage := &ChannelMessage{
		eventFields: eventFields,
		msg:         msg,
	}
	f.channel <- channelMessage
}
//func (f *Forwarder) ConsumeMessages() {
//	log := make(map[string]string)
//	for channelMessage := range f.channel {
//		//messages := Messages{}
//		//message := Message{
//		//	Text: channelMessage.msg,
//		//}
//		log["log"] = channelMessage.msg
//		fmt.Println(channelMessage)
//		for k, v := range channelMessage.eventFields {
//			if k == "timestamp" {
//				//message.Timestamp = v.(int64)
//
//
//			} else {
//				//message.Fields = append(message.Fields, Field{Name: f.CreateKey(k), Content: fmt.Sprint(v)})
//				log[k] = v.(string)
//			}
//		}
//
//		if f.hasJSONLogMsg {
//
//			var obj map[string]interface{}
//			msgbytes := []byte(channelMessage.msg)
//			err := json.Unmarshal(msgbytes, &obj)
//			if err == nil {
//				for k, v := range obj {
//					//message.Fields = append(message.Fields, Field{Name: f.CreateKey(k), Content: fmt.Sprint(v)})
//					log[k] = v.(string)
//				}
//			} else {
//				logging.LogError("Error unmarshalling", err)
//				return
//			}
//		}
//
//		//messages.Messages = append(messages.Messages, message)
//		payload ,err := json.Marshal(log)
//		if err == nil {
//			f.Post(*f.url,*f.token ,payload)
//		} else {
//			logging.LogError("Error marshalling", err)
//		}
//	}
//}

func (f *Forwarder) ConsumeMessages() {
	for channelMessage := range f.channel {
		channelMessage.eventFields["log"] = channelMessage.msg
		Logdata,err := json.Marshal(channelMessage.eventFields)
		//fmt.Println(fmt.Sprintf("%s",Logdata))
		if err == nil {
			f.Post(*f.url,*f.token ,Logdata)
		} else {
			logging.LogError("Error marshalling", err)
		}
	}
}


func (f *Forwarder) Post(url string, token string, payload []byte) {
	if f.debug {
		logging.LogStd("Post being sent", true)
	}

	client := &http.Client {
	}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(payload))

	if err != nil {
		fmt.Println(err)
		return
	}
	req.Header.Add("Authorization", token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		logging.LogError("Error Posting data", err)
		return
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	if f.debug {
		logging.LogStd(fmt.Sprintf("Post response code %s with body %s", resp.Status, body), true)
	}
}

type Messages struct {
	Messages []Message `json:"messages"`
}

type Message struct {
	Fields    []Field `json:"fields"`
	Text      string  `json:"text"`
	Timestamp int64   `json:"timestamp"`
}

type Field struct {
	Name    string `json:"name"`
	Content string `json:"content"`
}

type ChannelMessage struct {
	eventFields map[string]interface{}
	msg         string
}
