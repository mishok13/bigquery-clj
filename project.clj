(defproject io.screen6/bigquery "0.1.0-SNAPSHOT"
  :description "BigQuery API in Clojure"
  :url "http://github.com/screen6/bigquery"
  :license {:name "MIT"
            :url "http://opensource.org/licenses/MIT"}
  :profiles {:dev {:dependencies [[midje "1.6.3"]]}}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [com.google.apis/google-api-services-bigquery "v2-rev175-1.19.0"]
                 [org.clojure/tools.logging "0.3.1"]])
