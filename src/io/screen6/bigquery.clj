(ns io.screen6.bigquery
  "Google BigQuery API wrapper implemented in Clojure"
  (:require
   [clojure.tools.logging :as logging])
  (:import
   [com.google.api.client.auth.oauth2 Credential]
   [com.google.api.client.googleapis.json GoogleJsonResponseException]
   [com.google.api.client.googleapis.auth.oauth2
    GoogleClientSecrets GoogleCredential GoogleCredential$Builder]
   [com.google.api.client.googleapis.javanet GoogleNetHttpTransport]
   [com.google.api.client.json.jackson2 JacksonFactory]
   [com.google.api.services.bigquery
    BigqueryScopes Bigquery$Jobs$Insert Bigquery Bigquery$Builder]
   [com.google.api.services.bigquery.model
    Job GetQueryResultsResponse JobConfiguration JobConfigurationQuery
    JobReference TableReference]))

;; Forward-declaring private functions
(declare authorize!)
(declare connect!)
(declare make-query)

(defprotocol PBigQueryConnection
  (datasets [this]
    "List datasets available")
  (switch-dataset [this dataset]
    "Switch connection to a different dataset")
  (tables [this]
    "List tables available in current dataset")
  (execute! [this sql]
    "Execute the query")
  (table-exists? [this table]
    "Check if the table exists"))

(defrecord BigQueryConnection [connection project dataset]
  PBigQueryConnection
  (datasets [this]
    (->> (.. connection (datasets) (list project) (execute) (getDatasets))
         (filter (fn bigquery-dataset? [info] (= "bigquery#dataset" (get info "kind"))))
         (map (fn simple-dataset-name [info] (get-in info ["datasetReference" "datasetId"])))))
  (tables [this]
    (->> (.. connection (tables) (list project dataset) (execute) (getTables))
         (filter (fn bigquery-table? [info] (= "bigquery#table" (get info "kind"))))
         (map (fn simple-table-name [info] (get-in info ["tableReference" "tableId"]))))))

(defn bigquery
  "Create a BigQuery connection

  \"account\" can be found in \"credentials\" section, under \"Email
  address\" field. \"key\" should be a path to P12 key. Project and dataset specify which "
  [account key project dataset]
  ;; TODO: check connection, check if key path can be read, check if
  ;; project and dataset are correct
  (let [connection (connect! account key)]
    (BigQueryConnection. connection project dataset)))

(defn authorize!
  "Authorize the API user"
  [account key-path &
   {:keys [http-transport json-factory]
    :or {http-transport (GoogleNetHttpTransport/newTrustedTransport)
         json-factory (JacksonFactory/getDefaultInstance)}}]
  (-> (GoogleCredential$Builder.)
      (.setTransport http-transport)
      (.setJsonFactory json-factory)
      (.setServiceAccountPrivateKeyFromP12File (clojure.java.io/file key-path))
      (.setServiceAccountId account)
      (.setServiceAccountScopes (BigqueryScopes/all))
      (.build)))

(defn ^:private connect!
  "Connect to BigQuery under given account"
  [account key-path]
  (let [json-factory (JacksonFactory/getDefaultInstance)
        http-transport (GoogleNetHttpTransport/newTrustedTransport)]
    (.build (Bigquery$Builder. http-transport
                               json-factory
                               (authorize! account key-path
                                           :json-factory json-factory
                                           :http-transport http-transport)))))

(defn ^:private make-query [])
