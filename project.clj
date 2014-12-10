(defproject io.screen6/bigquery "0.1.0-SNAPSHOT"
  :description "BigQuery API in Clojure"
  :url "http://github.com/screen6/bigquery"
  :scm {:name "git"
        :url "https://github.com/screen6/bigquery-clj"}
  :license {:name "MIT"
            :url "http://opensource.org/licenses/MIT"}
  :profiles {:dev {:dependencies [[midje "1.6.3"]]
                   :plugins [[lein-midje "3.1.3"]
                             [lein-cloverage "1.0.2"]]}}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [com.google.apis/google-api-services-bigquery "v2-rev175-1.19.0"]
                 [org.clojure/tools.logging "0.3.1"]]
  :repositories {"snapshots" {:url "https://clojars.org/repo"
                              :username [:gpg :env]
                              :password [:gpg :env]}})
