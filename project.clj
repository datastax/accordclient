(defproject accordclient "0.0.3"
  :description "Performs Accord operations against a Cassandra cluster"
  :url "https://github.com/datastax/accordclient"
  :license {:name "The Apache Software License, Version 2.0"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/tools.cli "0.3.3"]
;                 [cc.qbits/alia "5.0.0-alpha7"]
                 [cc.qbits/alia "4.0.1"]
                 [ch.qos.logback/logback-classic "1.1.5"]]
  :main ^:skip-aot accordclient.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
