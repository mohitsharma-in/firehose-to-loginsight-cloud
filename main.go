package main

import (
	"fmt"
	"log"
	"os"

	"github.com/cloudfoundry-community/firehose-to-syslog/caching"
	"github.com/cloudfoundry-community/firehose-to-syslog/eventRouting"
	"github.com/cloudfoundry-community/firehose-to-syslog/firehoseclient"
	"github.com/cloudfoundry-community/firehose-to-syslog/logging"
	"github.com/cloudfoundry-community/firehose-to-syslog/uaatokenrefresher"
	"github.com/cloudfoundry-community/go-cfclient"
	"github.com/pivotalservices/firehose-to-loginsight/loginsight"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	debug                    = kingpin.Flag("debug", "Enable debug mode. This enables additional logging").Default("false").OverrideDefaultFromEnvar("DEBUG").Bool()
	apiEndpoint              = kingpin.Flag("api-endpoint", "Api endpoint address. For bosh-lite installation of CF: https://api.10.244.0.34.xip.io").OverrideDefaultFromEnvar("API_ENDPOINT").Required().String()
	dopplerEndpoint          = kingpin.Flag("doppler-endpoint", "Overwrite default doppler endpoint return by /v2/info").OverrideDefaultFromEnvar("DOPPLER_ENDPOINT").String()
	subscriptionID           = kingpin.Flag("subscription-id", "Id for the subscription.").Default("firehose-to-loginsight").OverrideDefaultFromEnvar("FIREHOSE_SUBSCRIPTION_ID").String()
	clientID                 = kingpin.Flag("client-id", "Client ID.").Default("admin").OverrideDefaultFromEnvar("FIREHOSE_CLIENT_ID").String()
	clientSecret             = kingpin.Flag("client-secret", "Client Secret.").Default("admin-client-secret").OverrideDefaultFromEnvar("FIREHOSE_CLIENT_SECRET").String()
	skipSSLValidation        = kingpin.Flag("skip-ssl-validation", "Please don't").Default("false").OverrideDefaultFromEnvar("SKIP_SSL_VALIDATION").Bool()
	keepAlive                = kingpin.Flag("fh-keep-alive", "Keep Alive duration for the firehose consumer").Default("25s").OverrideDefaultFromEnvar("FH_KEEP_ALIVE").Duration()
	logEventTotals           = kingpin.Flag("log-event-totals", "Logs the counters for all selected events since nozzle was last started.").Default("false").OverrideDefaultFromEnvar("LOG_EVENT_TOTALS").Bool()
	logEventTotalsTime       = kingpin.Flag("log-event-totals-time", "How frequently the event totals are calculated (in sec).").Default("30s").OverrideDefaultFromEnvar("LOG_EVENT_TOTALS_TIME").Duration()
	wantedEvents             = kingpin.Flag("events", fmt.Sprintf("Comma separated list of events you would like. Valid options are %s", eventRouting.GetListAuthorizedEventEvents())).Default("LogMessage").OverrideDefaultFromEnvar("EVENTS").String()
	boltDatabasePath         = kingpin.Flag("boltdb-path", "Bolt Database path ").Default("my.db").OverrideDefaultFromEnvar("BOLTDB_PATH").String()
	tickerTime               = kingpin.Flag("cc-pull-time", "CloudController Polling time in sec").Default("60s").OverrideDefaultFromEnvar("CF_PULL_TIME").Duration()
	extraFields              = kingpin.Flag("extra-fields", "Extra fields you want to annotate your events with, example: '--extra-fields=env:dev,something:other ").Default("").OverrideDefaultFromEnvar("EXTRA_FIELDS").String()
	logInsightServer         = kingpin.Flag("insight-server", "log insight server address").OverrideDefaultFromEnvar("INSIGHT_SERVER").String()
	logInsightServerPort     = kingpin.Flag("insight-server-port", "log insight server port").Default("9543").OverrideDefaultFromEnvar("INSIGHT_SERVER_PORT").Int()
	logInsightReservedFields = kingpin.Flag("insight-reserved-fields", "comma delimited list of fields that are reserved").Default("event_type").OverrideDefaultFromEnvar("INSIGHT_RESERVED_FIELDS").String()
	logInsightAgentID        = kingpin.Flag("insight-agent-id", "agent id for log insight").Default("1").OverrideDefaultFromEnvar("INSIGHT_AGENT_ID").String()
	logInsightHasJSONLogMsg  = kingpin.Flag("insight-has-json-log-msg", "app log message can be json").Default("false").OverrideDefaultFromEnvar("INSIGHT_HAS_JSON_LOG_MSG").Bool()
	concurrentWorkers        = kingpin.Flag("concurrent-workers", "number of concurrent workers pulling messages from channel").Default("50").OverrideDefaultFromEnvar("CONCURRENT_WORKERS").Int()
	noop                     = kingpin.Flag("noop", "if it should avoid sending to log-insight").Default("false").OverrideDefaultFromEnvar("INSIGHT_NOOP").Bool()
)

var (
	VERSION = "0.0.0"
)

func main() {
	kingpin.Version(VERSION)
	kingpin.Parse()

	var loggingClient logging.Logging
	//Setup Logging
	logging.LogStd(fmt.Sprintf("Starting firehose-to-loginsight %s ", VERSION), true)
	if len(*apiEndpoint) <= 0 {
		log.Fatal("Must set api-endpoint property")
		os.Exit(1)
	}
	if !*noop {
		if len(*logInsightServer) <= 0 {
			log.Fatal("Must set insight-server property")
			os.Exit(1)
		}
		loggingClient = loginsight.NewForwarder(*logInsightServer, *logInsightServerPort, *logInsightReservedFields, *logInsightAgentID, *logInsightHasJSONLogMsg, *debug, *concurrentWorkers)
	} else {
		loggingClient = loginsight.NewNoopForwarder()
	}

	c := cfclient.Config{
		ApiAddress:        *apiEndpoint,
		ClientID:          *clientID,
		ClientSecret:      *clientSecret,
		SkipSslValidation: *skipSSLValidation,
		UserAgent:         "firehose-to-loginsight/" + VERSION,
	}
	cfClient, err := cfclient.NewClient(&c)
	if err != nil {
		log.Fatal("New Client: ", err)
		os.Exit(1)

	}
	if len(*dopplerEndpoint) > 0 {
		cfClient.Endpoint.DopplerEndpoint = *dopplerEndpoint
	}

	logging.LogStd(fmt.Sprintf("Using %s as doppler endpoint", cfClient.Endpoint.DopplerEndpoint), true)

	//Creating Caching
	var cachingClient caching.Caching
	if caching.IsNeeded(*wantedEvents) {
		config := &caching.CachingBoltConfig{
			Path:               *boltDatabasePath,
			IgnoreMissingApps:  true,
			CacheInvalidateTTL: *tickerTime,
		}
		cachingClient, err = caching.NewCachingBolt(cfClient, config)
		if err != nil {
			log.Fatal("Error setting up caching client: ", err)
			os.Exit(1)
		}
	} else {
		cachingClient = caching.NewCachingEmpty()
	}
	//Creating Events
	events := eventRouting.NewEventRouting(cachingClient, loggingClient)
	err = events.SetupEventRouting(*wantedEvents)
	if err != nil {
		log.Fatal("Error setting up event routing: ", err)
		os.Exit(1)

	}

	//Set extrafields if needed
	events.SetExtraFields(*extraFields)

	//Enable LogsTotalevent
	if *logEventTotals {
		logging.LogStd("Logging total events", true)
		events.LogEventTotals(*logEventTotalsTime)
	}

	if err := cachingClient.Open(); err != nil {
		log.Fatal("Error open cache: ", err)
	}

	uaaRefresher, err := uaatokenrefresher.NewUAATokenRefresher(
		cfClient.Endpoint.AuthEndpoint,
		*clientID,
		*clientSecret,
		*skipSSLValidation,
	)

	if err != nil {
		logging.LogError(fmt.Sprint("Failed connecting to Get token from UAA..", err), "")
	}

	firehoseConfig := &firehoseclient.FirehoseConfig{
		TrafficControllerURL:   cfClient.Endpoint.DopplerEndpoint,
		InsecureSSLSkipVerify:  *skipSSLValidation,
		IdleTimeoutSeconds:     *keepAlive,
		FirehoseSubscriptionID: *subscriptionID,
	}

	if loggingClient.Connect() || *debug {

		logging.LogStd("Connecting to Firehose...", true)
		firehoseClient := firehoseclient.NewFirehoseNozzle(uaaRefresher, events, firehoseConfig)
		err = firehoseClient.Start()
		if err != nil {
			logging.LogError("Failed connecting to Firehose...Please check settings and try again!", err)

		} else {
			logging.LogStd("Firehose Subscription Succesfull! Routing events...", true)
		}

	} else {
		logging.LogError("Failed connecting Log Insight...Please check settings and try again!", "")
	}

	defer cachingClient.Close()
}
