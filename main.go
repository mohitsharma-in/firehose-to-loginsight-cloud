package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/cloudfoundry-community/firehose-to-syslog/authclient"
	"github.com/cloudfoundry-community/firehose-to-syslog/caching"
	"github.com/cloudfoundry-community/firehose-to-syslog/eventRouting"
	"github.com/cloudfoundry-community/firehose-to-syslog/firehoseclient"
	"github.com/cloudfoundry-community/firehose-to-syslog/logging"
	"github.com/cloudfoundry-community/firehose-to-syslog/stats"
	"github.com/cloudfoundry-community/go-cfclient"
	"github.com/cloudfoundry-incubator/uaago"
	"github.com/vmwarepivotallabs/firehose-to-loginsight/loginsight"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	debug                    = kingpin.Flag("debug", "Enable debug mode. This enables additional logging").Default("false").OverrideDefaultFromEnvar("DEBUG").Bool()
	apiEndpoint              = kingpin.Flag("api-endpoint", "Api endpoint address. For bosh-lite installation of CF: https://api.10.244.0.34.xip.io").OverrideDefaultFromEnvar("API_ENDPOINT").Required().String()
	subscriptionID           = kingpin.Flag("subscription-id", "Id for the subscription.").Default("firehose-to-loginsight").OverrideDefaultFromEnvar("FIREHOSE_SUBSCRIPTION_ID").String()
	clientID                 = kingpin.Flag("client-id", "Client ID.").Default("admin").OverrideDefaultFromEnvar("FIREHOSE_CLIENT_ID").String()
	clientSecret             = kingpin.Flag("client-secret", "Client Secret.").Default("admin-client-secret").OverrideDefaultFromEnvar("FIREHOSE_CLIENT_SECRET").String()
	skipSSLValidation        = kingpin.Flag("skip-ssl-validation", "Please don't").Default("false").OverrideDefaultFromEnvar("SKIP_SSL_VALIDATION").Bool()
	wantedEvents             = kingpin.Flag("events", fmt.Sprintf("Comma separated list of events you would like. Valid options are %s", eventRouting.GetListAuthorizedEventEvents())).Default("LogMessage").OverrideDefaultFromEnvar("EVENTS").String()
	boltDatabasePath         = kingpin.Flag("boltdb-path", "Bolt Database path ").Default("my.db").OverrideDefaultFromEnvar("BOLTDB_PATH").String()
	tickerTime               = kingpin.Flag("cc-pull-time", "CloudController Polling time in sec").Default("60s").OverrideDefaultFromEnvar("CF_PULL_TIME").Duration()
	extraFields              = kingpin.Flag("extra-fields", "Extra fields you want to annotate your events with, example: '--extra-fields=env:dev,something:other ").Default("").OverrideDefaultFromEnvar("EXTRA_FIELDS").String()
	logInsightServer         = kingpin.Flag("insight-server", "log insight server address").Default("data.mgmt.cloud.vmware.com").OverrideDefaultFromEnvar("INSIGHT_SERVER").String()
	logInsightServerPort     = kingpin.Flag("insight-server-port", "log insight server port").Default("443").OverrideDefaultFromEnvar("INSIGHT_SERVER_PORT").Int()
	logInsightServerToken    = kingpin.Flag("insight-server-token", "log insight server token").OverrideDefaultFromEnvar("INSIGHT_SERVER_TOKEN").String()
	logInsightReservedFields = kingpin.Flag("insight-reserved-fields", "comma delimited list of fields that are reserved").Default("event_type").OverrideDefaultFromEnvar("INSIGHT_RESERVED_FIELDS").String()
	logInsightHasJSONLogMsg  = kingpin.Flag("insight-has-json-log-msg", "app log message can be json").Default("false").OverrideDefaultFromEnvar("INSIGHT_HAS_JSON_LOG_MSG").Bool()
	concurrentWorkers        = kingpin.Flag("concurrent-workers", "number of concurrent workers pulling messages from channel").Default("50").OverrideDefaultFromEnvar("CONCURRENT_WORKERS").Int()
	noop                     = kingpin.Flag("noop", "if it should avoid sending to log-insight").Default("false").OverrideDefaultFromEnvar("INSIGHT_NOOP").Bool()
	bufferSize               = kingpin.Flag("logs-buffer-size", "Number of envelope to be buffered").Default("10000").Envar("LOGS_BUFFER_SIZE").Int()
	statServer               = kingpin.Flag("enable-stats-server", "Will enable stats server on 8080").Default("false").Envar("ENABLE_STATS_SERVER").Bool()
	orgs                     = kingpin.Flag("orgs", "Forwarded on the app logs from theses organisations' example: --orgs=org1,org2").Default("").Envar("ORGS").String()
	ignoreMissingApps        = kingpin.Flag("ignore-missing-apps", "Enable throttling on cache lookup for missing apps").Envar("IGNORE_MISSING_APPS").Default("false").Bool()
	stripAppSuffixes         = kingpin.Flag("strip-app-name-suffixes", "Suffixes that should be stripped from application names, comma separated").Envar("STRIP_APP_NAME_SUFFIXES").Default("").String()
)

var (
	VERSION = "0.0.1"
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
		loggingClient = loginsight.NewForwarder(*logInsightServer, *logInsightServerPort, *logInsightServerToken, *logInsightReservedFields, *logInsightHasJSONLogMsg, *debug, *concurrentWorkers, *skipSSLValidation)
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

	//Creating Caching
	var cacheStore caching.CacheStore
	switch {
	case boltDatabasePath != nil && *boltDatabasePath != "":
		cacheStore = &caching.BoltCacheStore{
			Path: *boltDatabasePath,
		}
	default:
		cacheStore = &caching.MemoryCacheStore{}
	}

	if err := cacheStore.Open(); err != nil {
		log.Fatal("Error open cache: ", err)
		os.Exit(1)
	}
	defer cacheStore.Close()

	cachingClient := caching.NewCacheLazyFill(
		&caching.CFClientAdapter{
			CF: cfClient,
		},
		cacheStore,
		&caching.CacheLazyFillConfig{
			IgnoreMissingApps:  *ignoreMissingApps,
			CacheInvalidateTTL: *tickerTime,
			StripAppSuffixes:   strings.Split(*stripAppSuffixes, ","),
		})

	if caching.IsNeeded(*wantedEvents) {
		// Bootstrap cache
		logging.LogStd("Pre-filling cache...", true)
		err = cachingClient.FillCache()
		if err != nil {
			log.Fatal("Error pre-filling cache: ", err)
			os.Exit(1)
		}
		logging.LogStd("Cache filled.", true)
	}

	//Adding Stats
	statistic := stats.NewStats()
	go statistic.PerSec()

	////Starting Http Server for Stats
	if *statServer {
		Server := &stats.Server{
			Logger: log.New(os.Stdout, "", 1),
			Stats:  statistic,
		}

		go Server.Start()
	}

	//Creating Events
	eventFilters := []eventRouting.EventFilter{eventRouting.HasIgnoreField, eventRouting.NotInCertainOrgs(*orgs)}
	events := eventRouting.NewEventRouting(cachingClient, loggingClient, statistic, eventFilters)
	err = events.SetupEventRouting(*wantedEvents)
	if err != nil {
		log.Fatal("Error setting up event routing: ", err)
		os.Exit(1)
	}

	//Set extrafields if needed
	events.SetExtraFields(*extraFields)

	logStreamAddress := strings.Replace(cfClient.Config.ApiAddress, "api", "log-stream", 1)
	logging.LogStd("Using LogStream address:"+logStreamAddress, true)
	firehoseConfig := &firehoseclient.FirehoseConfig{
		RLPAddr:                logStreamAddress,
		InsecureSSLSkipVerify:  *skipSSLValidation,
		FirehoseSubscriptionID: *subscriptionID,
		BufferSize:             *bufferSize,
	}

	if loggingClient.Connect() || *debug {
		logging.LogStd("Connected to Syslog Server! Connecting to Firehose...", true)
	} else {
		log.Fatal("Failed connecting to the Syslog Server...Please check settings and try again!", "")
		os.Exit(1)
	}

	uaa, err := uaago.NewClient(cfClient.Endpoint.AuthEndpoint)
	if err != nil {
		logging.LogError(fmt.Sprint("Failed connecting to Get token from UAA..", err), "")
	}

	ac := authclient.NewHttp(uaa, *clientID, *clientSecret, *skipSSLValidation)
	firehoseClient := firehoseclient.NewFirehoseNozzle(events, firehoseConfig, statistic, ac)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	firehoseClient.Start(ctx)

	signalChan := make(chan os.Signal, 1)
	cleanupDone := make(chan bool)
	signal.Notify(signalChan, os.Interrupt, os.Kill)
	go func() {
		for range signalChan {
			fmt.Println("\nSignal Received, Stop reading and starting Draining...")
			firehoseClient.StopReading()
			cctx, tcancel := context.WithTimeout(context.TODO(), 30*time.Second)
			tcancel()
			firehoseClient.Draining(cctx)
			cleanupDone <- true
		}
	}()
	<-cleanupDone

}
