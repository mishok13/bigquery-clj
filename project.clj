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
  :dependencies [[base64-clj "0.1.1"]
                 [cheshire "5.4.0"]
                 [clj-http "1.0.1"]
                 [clj-time "0.8.0"]
                 [com.cemerick/url "0.1.1"]
                 [com.google.apis/google-api-services-bigquery "v2-rev175-1.19.0"]
                 [org.bouncycastle/bcpkix-jdk15on "1.51"]
                 [org.bouncycastle/bcprov-jdk15on "1.51"]
                 [org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [pandect "0.4.1"]]
  :repositories {"snapshots" {:url "https://clojars.org/repo"
                              :username [:gpg :env]
                              :password [:gpg :env]}})
