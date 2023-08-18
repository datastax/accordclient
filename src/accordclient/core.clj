(ns accordclient.core
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [qbits.alia :as alia])
  (:import (com.datastax.driver.core.exceptions UnavailableException
                                                WriteTimeoutException
                                                ReadTimeoutException
                                                OperationTimedOutException
                                                NoHostAvailableException))
  (:gen-class))

(def cli-options
  [["-r" "--register-set SET" "Set of registers to operate against, as comma-separated list like 3,4,5"
    :default [(int 1)]
    :parse-fn (fn [arg]
                  (as-> arg v
                        (str/split v #",")
                        (map #(Integer/parseInt %) v)
                        (set v)
                        (vec v)))
    :validate [#(seq %) "Must provide at least one register"]]
   ["-t" "--thread-count COUNT" "Number of LWT threads to run concurrently"
    :default 1
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 %) "Must be greater than 0"]]
   ["-s" "--start-time TIME" "Starting relative time in nanoseconds"
    :default 0
    :parse-fn #(Long/parseLong %)]
   ["-n" "--operation-count COUNT" "Number of operations to perform"
    :default 10000
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 %) "Must be greater than 0"]]
   ["-u" "--upper-bound BOUND" "Upper bound of values a register can hold"
    :default 5
    :parse-fn #(Integer/parseInt %)
    :validate [#(< -1 %) "Must be greater than or equal to 0"]]
   ["-p" "--print-schema" "Schema to use for cluster under load"]
   [nil "--read-timeout TIME" "Driver read timeout"
    :default 12000
    :parse-fn #(Long/parseLong %)]
   ["-H" "--hosts HOSTS" "Hosts to contact"
    :default ["localhost"]
    :parse-fn #(str/split % #",")
    :validate [#(seq %) "Must provide at least one host"]]
   [nil "--cas-register"]
   [nil "--rw-register"]
   [nil "--list-append"]
   ["-h" "--help"]])

(def ak (keyword "row.contents"))
(def ak1 (keyword "row1.contents"))
(def ak2 (keyword "row2.contents"))

(defn ^Long linear-time-nanos
  "A linear time source in nanoseconds."
  []
  (System/nanoTime)
  )

(defn print-schema
  "Prints schema for use"
  []
  (println (slurp (io/resource "accordclient_schema.cql"))))

(defn- lprintln
  "Println that locks *out*"
  [& args]
  (locking *out*
    (apply println args)))

(let [make-op-lock (Object.)]
  (defn- make-op
    "Build op, timestamp it, print it, and return it"
    [op time-fn]
    (locking make-op-lock
      (let [timestamped-op (assoc op :time (time-fn))]
        (println timestamped-op)
        timestamped-op))))

(defn handle-driver-exceptions
  [exception op]
  (let [driver-exception (.getCause exception)]
    (cond
     (instance? UnavailableException driver-exception) (assoc op :type :fail :cause :unavailable)
     (instance? ReadTimeoutException driver-exception) (assoc op :type :info :cause :read-timed-out)
     (instance? WriteTimeoutException driver-exception) (assoc op :type :info :cause :write-timed-out)
     (instance? OperationTimedOutException driver-exception) (assoc op :type :info :cause :op-timed-out) ; thrown by the client when it didn’t hear back from the coordinator within the driver read timeout
     (instance? NoHostAvailableException driver-exception) (do (Thread/sleep 1000) (assoc op :type :fail :cause :nohost))
     :else (assoc op :type :error :cause :unhandled-exception :details (.toString exception))
     )
    )
  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;; Functions supporting the "cas-register" model (for Knossos)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn cas-reg
  "CAS command"
  [session pst op]
  (try
    (let [[v v'] (:value op)
          register (:register op)
          result (alia/execute session pst {:values [register v v' register]})]
      (if (= v (-> result first ak))
        (assoc op :type :ok)
        (assoc op :type :fail)))
    (catch Exception e
      (handle-driver-exceptions e op)
      )
    )
  )

(defn cas-reg-write
  "Update if exists, otherwise insert"
  [session pst pst-non-existent op]
  (try
    (let [v (:value op)
          register (:register op)
          result (alia/execute session pst {:values [register v register]})]
      (if (not (nil? (-> result first ak)))
        (assoc op :type :ok)
        (let [result' (alia/execute session pst-non-existent {:values [register register v]})]
          (if (nil? (-> result' first ak))
            (assoc op :type :ok)
            (assoc op :type :fail))
          )
        )
      )
    (catch Exception e
      (handle-driver-exceptions e op)
      )
    )
  )

(defn cas-reg-read
  "Read from the CAS register"
  [session pst op]
  (try
    (let [value (-> (alia/execute session pst {:values [(:register op)]}) first ak)]
      (assoc op :type :ok :value value)
      )
    (catch Exception e
      (handle-driver-exceptions e op)
      )
    )
  )

(defn do-cas-register-writes
  "Run CAS operations against a cluster and check with Knossos"
  [hosts time-base register-set upper-bound count process-counter read-timeout]
  (try
    (let [cluster (alia/cluster {:contact-points hosts, :socket-options {:read-timeout read-timeout}})
          session (alia/connect cluster)
          _ (alia/execute session "USE accord;")
          corrected-time (fn [] (+ time-base (linear-time-nanos)))
          prepared-read (alia/prepare session "
          BEGIN TRANSACTION
            LET row = (SELECT * FROM cas_registers WHERE id = ?);
            SELECT row.contents;
          COMMIT TRANSACTION;"
                                      )
          ;"UPDATE registers SET contents=? WHERE id=? IF EXISTS")
          prepared-write (alia/prepare session "
          BEGIN TRANSACTION
            LET row = (SELECT * FROM cas_registers WHERE id=?);
            SELECT row.contents;
            IF row IS NOT NULL THEN
              UPDATE cas_registers SET contents=? WHERE id=?;
            END IF
          COMMIT TRANSACTION;"
                                       )
          ;"INSERT INTO registers (id, contents) VALUES (?, ?) IF NOT EXISTS")
          prepared-write-not-exists (alia/prepare session "
          BEGIN TRANSACTION
            LET row = (SELECT * FROM cas_registers WHERE id=?);
            SELECT row.contents;
            IF row IS NULL THEN
              INSERT INTO cas_registers(id, contents) VALUES (?, ?);
            END IF
          COMMIT TRANSACTION;"
                                                  )
          ;"UPDATE registers SET contents=? WHERE id=? IF contents=?")]
          prepared-cas (alia/prepare session "
          BEGIN TRANSACTION
            LET row = (SELECT * FROM cas_registers WHERE id = ?);
            SELECT row.contents;
            IF row.contents = ? THEN
              UPDATE cas_registers SET contents = ? WHERE id = ?;
            END IF
          COMMIT TRANSACTION;"
                                     )
          ]
      (loop [n 1
             my-process (swap! process-counter inc)]
        (let [f (rand-nth [:cas :write :read])
              value (case f
                      :cas [(rand-int upper-bound) (rand-int upper-bound)]
                      :write (rand-int upper-bound)
                      :read nil)
              op (make-op {:type :invoke :f f
                           :process my-process
                           :value value
                           :register (rand-nth register-set)}
                          corrected-time)
              new-op (make-op (case f
                                :cas (cas-reg session prepared-cas op)
                                :write (cas-reg-write session prepared-write prepared-write-not-exists op)
                                :read (cas-reg-read session prepared-read op))
                              corrected-time)]
          (when (< n count)
            (recur (inc n) (if (= :info (:type new-op))
                               (swap! process-counter inc)
                               my-process)))))
      (alia/shutdown session)
      (alia/shutdown cluster))
    (catch Exception e
      (println e)
      (System/exit 1))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;; Functions supporting the "list-append" model (for Elle)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn la-read
  "Single read from the list-append model"
  [session pst op]
  (try
    (let
      [
        register (get-in (:value op) [0 1])
        value (-> (alia/execute session pst {:values [register]}) first ak)
        ]
      (assoc op :type :ok :value [[:r register value]])
      )
    (catch Exception e
      (handle-driver-exceptions e op)
      )
    )
  )

(defn la-write
  "Single write to the list-append model"
  [session stmt op]
  (try
    (let [
           result (alia/execute session stmt)
           ]
      (if (= [] (-> result first ak))
        (assoc op :type :ok)
        (assoc op :type :fail)))
    (catch Exception e
      (handle-driver-exceptions e op)
      )
    )
  )

(defn do-list-append-writes
  "Run read-append operations from/to a list against a cluster to check with Elle"
  [hosts time-base register-set thread_id count process-counter read-timeout]
  (try
    (let [cluster (alia/cluster {:contact-points hosts, :socket-options {:read-timeout read-timeout}})
          session (alia/connect cluster)
          _ (alia/execute session "USE accord;")
          corrected-time (fn [] (+ time-base (linear-time-nanos)))
          prepared-read (alia/prepare session "
          BEGIN TRANSACTION
            LET row = (SELECT * FROM list_append WHERE id = ?);
            SELECT row.contents;
          COMMIT TRANSACTION;"
                                      )
          write-stmt-tmpl "
          BEGIN TRANSACTION
            LET row = (SELECT * FROM list_append WHERE id=0);
            SELECT row.contents;
            IF row IS NULL THEN
              UPDATE list_append SET contents += [%d] WHERE id = %d;
            END IF
          COMMIT TRANSACTION;"
          ]
      (loop [n 1
             my-process (swap! process-counter inc)]
        (let [f (rand-nth [:read :write])
              register (rand-nth register-set)
              register-value (+ (* thread_id count) n) ;makes value unique across all threads
              write-stmt (format write-stmt-tmpl register-value register)
              value (case f
                          :read [[:r register nil]]
                          :write [[:append register register-value]])
              op (make-op {:type :invoke
                           :process my-process
                           :value value
                           :tid thread_id
                           :n n
                           }
                          corrected-time)
              new-op (make-op (case f
                                    :read (la-read session prepared-read op)
                                    :write (la-write session write-stmt op)
                                    )
                              corrected-time)
              ]
          (when (< n count)
                (recur (inc n) (if (= :info (:type new-op))
                                 (swap! process-counter inc)
                                 my-process))
                )
          )
        )
      (alia/shutdown session)
      (alia/shutdown cluster))
    (catch Exception e
      (println e)
      (System/exit 1))
    )
  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;; Functions supporting the "rw-register" model (for Elle)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn rw-reg-read
  "Single read from the rw-register model"
  [session pst op]
  (try
    (let
      [
        register (get-in (:value op) [0 1])
        value (-> (alia/execute session pst {:values [register]})
                  first ak)
        ]
      (assoc op :type :ok :value [[:r register value]]))
    (catch Exception e
      (handle-driver-exceptions e op)
      )
    )
  )

(defn rw-reg-write
  "Single write to the rw-register model"
  [session pst op]
  (try
    (let [
           register (get-in (:value op) [0 1])
           value (get-in (:value op) [0 2])
           result (alia/execute session pst {:values [register value]})]
      (if (= nil (-> result first ak))
        (assoc op :type :ok)
        (assoc op :type :fail)))
    (catch Exception e
      (handle-driver-exceptions e op)
      )
    )
  )

(defn rw-reg-single-rr-mix
  "Double read from the rw-register model"
  [session pst op]
  (try
    (let [
           r1-register (get-in (:value op) [0 1])
           r2-register (get-in (:value op) [1 1])
           result (alia/execute session pst {:values [r1-register r2-register]})
           r1-result (-> result first ak1)
           r2-result (-> result first ak2)
           ]
      (assoc (assoc-in (assoc-in op [:value 0 2] r1-result) [:value 1 2] r2-result) :type :ok)
      )
    (catch Exception e
      (handle-driver-exceptions e op)
      )
    )
  )

(defn rw-reg-single-rw-mix
  "Read followed by write to the rw-register model"
  [session pst op]
  (try
    (let [
           r-register (get-in (:value op) [0 1])
           w-register (get-in (:value op) [1 1])
           value (get-in (:value op) [1 2])
           result (-> (alia/execute session pst {:values [r-register w-register value]}) first ak)
           ]
      (assoc (assoc-in op [:value 0 2] result) :type :ok)
      )
    (catch Exception e
      (handle-driver-exceptions e op)
      )
    )
  )

(defn rw-reg-single-ww-mix
  "Double write to the rw-register model"
  [session pst op]
  (try
    (let [
           w1-register (get-in (:value op) [0 1])
           w2-register (get-in (:value op) [1 1])
           w1-register-value (get-in (:value op) [0 2])
           w2-register-value (get-in (:value op) [1 2])
           result (-> (alia/execute session pst {:values [w1-register w1-register-value w2-register w2-register-value]}))
           ]
      (if (= nil (-> result first ak))
        (assoc op :type :ok)
        (assoc op :type :fail)
        )
      )
    (catch Exception e
      (handle-driver-exceptions e op)
      )
    )
  )

(defn do-rw-register-writes
  "Run read-write operations from/to a rgister against a cluster to check with Elle"
  [hosts time-base register-set thread_id count max-ops-per-tx process-counter read-timeout]
  (try
    (let [cluster (alia/cluster {:contact-points hosts, :socket-options {:read-timeout read-timeout} })
          session (alia/connect cluster)
          _ (alia/execute session "USE accord;")
          corrected-time (fn [] (+ time-base (linear-time-nanos)))
          prepared-read (alia/prepare session "
          BEGIN TRANSACTION
            LET row = (SELECT * FROM rw_registers WHERE id = ?);
            SELECT row.contents;
          COMMIT TRANSACTION;"
                                      )
          prepared-write (alia/prepare session "
          BEGIN TRANSACTION
            LET row = (SELECT * FROM rw_registers WHERE id=0);
            SELECT row.contents;
            IF row IS NULL THEN
              INSERT INTO rw_registers(id, contents) VALUES (?, ?);
            END IF
          COMMIT TRANSACTION;"
                                       )
          prepared-single-rr-mix (alia/prepare session "
          BEGIN TRANSACTION
            LET row1 = (SELECT * FROM rw_registers WHERE id=?);
            LET row2 = (SELECT * FROM rw_registers WHERE id=?);
            SELECT row1.contents,row2.contents;
          COMMIT TRANSACTION;"
                                       )
          prepared-single-rw-mix (alia/prepare session "
          BEGIN TRANSACTION
            LET row = (SELECT * FROM rw_registers WHERE id=?);
            SELECT row.contents;
            INSERT INTO rw_registers(id, contents) VALUES (?, ?);
          COMMIT TRANSACTION;"
                                       )
          prepared-single-ww-mix (alia/prepare session "
          BEGIN TRANSACTION
            LET row = (SELECT * FROM rw_registers WHERE id=0);
            SELECT row.contents;
            INSERT INTO rw_registers(id, contents) VALUES (?, ?);
            INSERT INTO rw_registers(id, contents) VALUES (?, ?);
          COMMIT TRANSACTION;"
                                       )
          threshold (* max-ops-per-tx thread_id count)
          ]
      (loop [n 1
             v (atom 1)
             my-process (swap! process-counter inc)]
        (let [f (rand-nth [:read, :write, :single-ww-mix, :single-rr-mix, :single-rw-mix ])
              value (case f
                          :write
                          (let [
                                 register (rand-nth register-set)
                                 register-value (+ threshold @v) ;makes value unique across all threads
                                 ]
                            (swap! v inc)
                            [[:w register register-value]]
                            )
                          :read
                          (let [register (rand-nth register-set)]
                            [[:r register nil]]
                            )
                          :single-rr-mix
                          (let [
                                 r1-register (rand-nth register-set)
                                 r2-register (rand-nth register-set)
                                 ]
                            [[:r r1-register nil] [:r r2-register nil]]
                            )
                          :single-rw-mix
                          (let [
                                 r-register (rand-nth register-set)
                                 w-register (rand-nth register-set)
                                 w-register-value (+ threshold @v) ;makes value unique across all threads
                                 ]
                            (swap! v inc)
                            [[:r r-register nil] [:w w-register w-register-value]]
                            )
                          :single-ww-mix
                          (let [
                                 w1-register (rand-nth register-set)
                                 w2-register (rand-nth register-set)
                                 w1-register-value (+ threshold @v) ;makes value unique across all threads
                                 w2-register-value (+ threshold (swap! v inc)) ;makes value unique across all threads
                                 ]
                            (swap! v inc)
                            [[:w w1-register w1-register-value] [:w w2-register w2-register-value]]
                            )


                          :single-wr-mix; TODO this is not possible with Accord right now. We can't UPDATE and then SELECT the written value. This affects also the mix case below, where `r` ops can be only present before any `w1` ops.
                          :mix; TODO the below is not finished yet -> need to concatenate the CQL statement based on the `value` below. If there are no `r` ops, then add a select where id = 0; if there are any `r` ops, then they all must preeced any of the `w` ops (see NOTE above re: single-wr-mix)
                          (let [
                                      ;number-ops-per-tx (rand-int max-ops-per-tx)
                                      number-ops-per-tx 3; TODO replace with ^

                                      ]
                                 ; return array of arrays
                                 ;[[:r 1 nil] [:r 2 nil] [:w 2 1] [:w 1 2]]
                                 (vec (for [i (range number-ops-per-tx)]
                                        (let [
                                               op (rand-nth (vector :w :r))
                                               register (rand-nth register-set)
                                               register-value @v
                                               ]
                                          (swap! v inc)
                                        [op register register-value]
                                        )
                                        ))
                                 )
                          )
              op (make-op {:type :invoke :f "txn"
                           :process my-process
                           :value value
                           :tid thread_id
                           :step n
                           }
                          corrected-time)
              new-op (make-op (case f
                                    :read (rw-reg-read session prepared-read op)
                                    :write (rw-reg-write session prepared-write op)
                                    :single-rr-mix (rw-reg-single-rr-mix session prepared-single-rr-mix op)
                                    :single-rw-mix (rw-reg-single-rw-mix session prepared-single-rw-mix op)
                                    :single-ww-mix (rw-reg-single-ww-mix session prepared-single-ww-mix op)
                                    )
                              corrected-time)
              ]
          (when (< n count)
                (recur (inc n) v (if (= :info (:type new-op))
                                 (swap! process-counter inc)
                                 my-process)))
          ))
      (alia/shutdown session)
      (alia/shutdown cluster))
    (catch Exception e
      (println e)
      (System/exit 1))
    )
  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;; main entry-point for the CLI app
; run the re-register model via:
; lein run --rw-register -t 1 -r 1,2,3,4,5 -n 1 -H 172.17.0.2 -s 1000
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn -main
  "Entry point for CLI app"
  [& args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)]
    (cond
      (:help options) (println summary)
      (:print-schema options) (print-schema)
      errors (do (println summary) (println errors) (System/exit 1))
      (:cas-register options) (do
        (let [{:keys [hosts start-time upper-bound register-set
                      operation-count thread-count read-timeout]} options
                      count (quot operation-count thread-count)
                      process-counter (atom 0)
                      modified-start-time (- start-time (linear-time-nanos))]
          (dotimes [_ thread-count]
            (future (do-cas-register-writes hosts modified-start-time register-set upper-bound count process-counter read-timeout)))
          (shutdown-agents))

        )

      (:rw-register options) (do
        (let [{:keys [hosts start-time register-set
                      operation-count thread-count read-timeout]} options
                      count (quot operation-count thread-count)
                      process-counter (atom 0)
                      max-ops-per-tx 5
                      modified-start-time (- start-time (linear-time-nanos))]
          (dotimes [thread_id thread-count]
            (future (do-rw-register-writes hosts modified-start-time register-set thread_id count max-ops-per-tx process-counter read-timeout)))
          (shutdown-agents))
      )
      (:list-append options) (do
        (let [{:keys [hosts start-time register-set
                      operation-count thread-count read-timeout]} options
                      count (quot operation-count thread-count)
                      process-counter (atom 0)
                      modified-start-time (- start-time (linear-time-nanos))]
          (dotimes [thread_id thread-count]
            (future (do-list-append-writes hosts modified-start-time register-set thread_id count process-counter read-timeout)))
          (shutdown-agents))
      )
      :else
      (System/exit 1))))
