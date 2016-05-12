(ns jruby-profile-aggregator.core
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.tools.cli :as cli]
            [cheshire.core :as cheshire]
            [me.raynes.fs :as fs]))

(defn find-json-files
  [dir extra-filter]
  (filter #(let [path (.getPath %)]
            (and (.endsWith path ".json")
                 (or (nil? extra-filter)
                     (not (re-find (re-pattern extra-filter) path)))))
          (file-seq (io/file dir))))

(defn update-method-entry
  [entry method-data]
  (let [total-time (get method-data "total_time")
        self-time (get method-data "self_time")
        child-time (get method-data "child_time")
        total-calls (get method-data "total_calls")]
    (let [updated
          (if entry
            (-> (update entry
                        :total-time
                        +
                        total-time)
                (update :self-time
                        +
                        self-time)
                (update :child-time
                        +
                        child-time)
                (update :total-calls
                        +
                        total-calls))
            {:total-time total-time
             :self-time self-time
             :child-time child-time
             :total-calls total-calls})]
      updated)))

(defn update-summary-totals-for-file-entry
  [entry profile-data]
  (let [total-time (get profile-data "total_time")]
    (if entry
      (->
       (update entry
               :total-time
               +
               total-time)
       (update :total-calls inc))
      {:total-time total-time
       :total-calls 1
       :methods {}})))

(defn update-file-entry
  [entry profile-data]
  (reduce
   (fn [acc method-info]
     (update-in acc
                [:methods (get method-info "name")]
                update-method-entry
                method-info))
   (update-summary-totals-for-file-entry entry profile-data)
   (get profile-data "methods")))

(defn sum-endpoint-info-for-dir-or-file
  [dir file-filter]
  (reduce
   (fn [acc json-file]
     (update acc
             (subs (.getParent json-file) (count dir))
             update-file-entry
             (cheshire/parse-stream (io/reader json-file))))
   {}
   (find-json-files dir file-filter)))

(defn add-mean-total-time-per-call-to-methods
  [methods]
  (reduce
   (fn [acc method-info]
     (let [method-detail (val method-info)]
       (assoc acc (key method-info)
                  (assoc
                   method-detail
                    :mean-total-time-per-call
                    (/ (:total-time method-detail)
                       (:total-calls method-detail))))))
   {}
   methods))

(defn average-endpoint-info-for-dir
  [dir file-filter]
  (reduce
   (fn [acc endpoint]
     (assoc acc (key endpoint)
                (update-in (val endpoint)
                           [:methods]
                           add-mean-total-time-per-call-to-methods)))
   {}
   (sum-endpoint-info-for-dir-or-file dir file-filter)))

(defn filter-for-methods-having-greater-total-time-per-endpoint-call
  [methods-in-comparison
   base
   endpoint-name
   total-endpoint-calls-in-comparison
   total-endpoint-calls-in-base]
  (reduce
   (fn [acc method]
     (let [method-name (key method)
           total-time-per-endpoint-call-in-comparison (/
                                                       (:total-time
                                                        (val method))
                                                       total-endpoint-calls-in-comparison)
           total-time-in-base (get-in base [endpoint-name
                                            :methods
                                            method-name
                                            :total-time])
           total-time-increase-per-endpoint-call-over-base (if
                                                             (nil?
                                                              total-time-in-base)
                                                              total-time-per-endpoint-call-in-comparison
                                                              (-
                                                               total-time-per-endpoint-call-in-comparison
                                                               (/
                                                                total-time-in-base
                                                                total-endpoint-calls-in-base)))]
       (if (pos? total-time-increase-per-endpoint-call-over-base)
         (assoc acc
           method-name
           (assoc (val method)
             :total-time-increase-per-endpoint-call-over-base
             total-time-increase-per-endpoint-call-over-base
             :total-time-per-endpoint-call
             total-time-per-endpoint-call-in-comparison))
         acc)))
   {}
   methods-in-comparison))

(defn filter-for-endpoint-methods-having-greater-total-time
  [base comparison]
  (reduce
   (fn [acc endpoint]
     (let [endpoint-name (key endpoint)
           endpoint-val (val endpoint)
           base-total-calls (get-in base [endpoint-name :total-calls] 0)
           base-total-time (get-in base [endpoint-name :total-time] 0)
           mean-base-time-per-call (if (pos? base-total-calls)
                                     (/ base-total-time base-total-calls)
                                     0)
           comparison-total-calls (:total-calls endpoint-val)
           mean-comparison-time-per-call (/ (:total-time endpoint-val)
                                            comparison-total-calls)]
       (assoc acc
         endpoint-name
         {:mean-total-time-increase-over-base (- mean-comparison-time-per-call
                                                 mean-base-time-per-call)
          :mean-base-time-per-call mean-base-time-per-call
          :mean-comparison-time-per-call mean-comparison-time-per-call
          :total-base-calls base-total-calls
          :total-comparison-calls comparison-total-calls
          :methods (filter-for-methods-having-greater-total-time-per-endpoint-call
                    (:methods endpoint-val)
                    base
                    endpoint-name
                    comparison-total-calls
                    base-total-calls)})))
   {}
   comparison))

(defn flip-method-base-and-comparison-elements
  [methods]
  (mapv
   #(-> %
       (assoc :total-time-increase-per-endpoint-call-over-comparison
              (:total-time-increase-per-endpoint-call-over-base %))
       (dissoc :total-time-increase-per-endpoint-call-over-base))
   methods))

(defn flip-endpoint-base-and-comparison-elements
  [endpoints]
  (mapv
   #(let [{:keys [mean-total-time-increase-over-base
                  mean-base-time-per-call
                  mean-comparison-time-per-call
                  total-base-calls
                  total-comparison-calls
                  methods
                  name]}
          %]
     {:name name
      :mean-total-time-increase-over-comparison
      mean-total-time-increase-over-base
      :mean-base-time-per-call mean-comparison-time-per-call
      :mean-comparison-time-per-call mean-base-time-per-call
      :total-base-calls total-comparison-calls
      :total-comparison-calls total-base-calls
      :methods (flip-method-base-and-comparison-elements methods)})
   endpoints))

(defn sort-methods
  [methods method-sort-keys]
  (sort-by method-sort-keys
           (fn [key1 key2]
             (compare key2 key1))
           (mapv
            (fn [method-info]
              (assoc
               (val method-info)
                :name
                (key method-info)))
            methods)))

(defn sort-methods-in-endpoints
  [endpoints endpoint-sort-keys method-sort-keys]
  (sort-by endpoint-sort-keys
           (fn [key1 key2]
             (compare key2 key1))
           (mapv
            (fn [endpoint]
              (->
               (update (val endpoint)
                       :methods
                       sort-methods
                       method-sort-keys)
               (assoc
                 :name
                 (key endpoint))))
            endpoints)))

(defn hash-methods-by-name
  [methods]
  (reduce
   (fn [acc method-info]
     (assoc acc (:name method-info)
                (dissoc method-info :name)))
   {}
   methods))

(defn hash-endpoints-by-name
  [endpoints]
  (reduce
   (fn [acc endpoint-info]
     (assoc acc (:name endpoint-info)
                (-> endpoint-info
                    (update :methods
                            hash-methods-by-name)
                    (dissoc :name))))
   {}
   endpoints))

(defn load-average-endpoint-info-from-json-file
  [input-file]
  (hash-endpoints-by-name
   (cheshire/parse-stream (io/reader input-file) true)))

(defn write-to-json-file
  [profile-data output-file]
  (cheshire/generate-stream
   profile-data
   (io/writer output-file)))

(defn write-to-flat-file
  [profile output-file flip-base-vs-comparison-data?]
  (with-open [wrtr (io/writer output-file)]
    (.write wrtr (apply str (repeat 35 "-")))
    (.newLine wrtr)
    (.write wrtr "endpoint summary")
    (.newLine wrtr)
    (.newLine wrtr)
    (.write wrtr (string/join "  "
                              (if flip-base-vs-comparison-data?
                                ["mean inc over comp"
                                 "mean total time per call"
                                 "calls in comp"
                                 "calls in base"
                                 "endpoint name"]
                                ["mean inc over base"
                                 "mean total time per call"
                                 "calls in base"
                                 "calls in comp"
                                 "endpoint name"])))
    (.newLine wrtr)
    (.write wrtr (apply str (repeat 100 "-")))
    (.newLine wrtr)
    (doseq [{:keys [mean-total-time-increase-over-base
                    mean-comparison-time-per-call
                    total-base-calls
                    total-comparison-calls
                    name]} profile]
      (.write wrtr (string/join
                    "  "
                    [(format "%18.6f" mean-total-time-increase-over-base)
                     (format "%24.6f" mean-comparison-time-per-call)
                     (format "%13d" total-base-calls)
                     (format "%13d" total-comparison-calls)
                     name]))
      (.newLine wrtr))
    (.newLine wrtr)
    (.write wrtr (apply str (repeat 35 "-")))
    (.newLine wrtr)
    (.write wrtr "summary per-endpoint")
    (.newLine wrtr)
    (.newLine wrtr)
    (doseq [endpoint profile]
      (.write wrtr (:name endpoint))
      (.newLine wrtr)
      (.newLine wrtr)
      (.write wrtr (string/join "  "
                                (if flip-base-vs-comparison-data?
                                  ["inc over comp per endpoint call"
                                   "total time per endpoint call"
                                   "method name"]
                                  ["inc over base per endpoint call"
                                   "total time per endpoint call"
                                   "method name"])))
      (.newLine wrtr)
      (.write wrtr (apply str (repeat 70 "-")))
      (.newLine wrtr)
      (doseq [{:keys [total-time-increase-per-endpoint-call-over-base
                      total-time-per-endpoint-call
                      name]}
              (:methods endpoint)]
        (.write wrtr (string/join
                      "  "
                      [(format "%31.6f"
                               total-time-increase-per-endpoint-call-over-base)
                       (format "%28.6f" total-time-per-endpoint-call)
                       name]))
        (.newLine wrtr))
      (.newLine wrtr))))

(defn process-one-profile-dir
  [base-dir output-dir file-filter]
  (let [output-file (str output-dir "/one-aggregated-profile.json")]
    (write-to-json-file
     (sort-methods-in-endpoints
      (average-endpoint-info-for-dir base-dir file-filter)
      :total-time
      :total-time)
     output-file)
    output-file))

(defn compare-profile-dirs
  [base-profile-data compare-profile-data output-dir]
  (let [base-greater-than-compare-data
        (sort-methods-in-endpoints
         (filter-for-endpoint-methods-having-greater-total-time
          compare-profile-data
          base-profile-data)
         (juxt
          :mean-total-time-increase-over-base
          :mean-comparison-time-per-call)
         (juxt
          :total-time-increase-per-endpoint-call-over-base
          :total-time-per-endpoint-call))
        compare-greater-than-base-data
        (sort-methods-in-endpoints
         (filter-for-endpoint-methods-having-greater-total-time
          base-profile-data
          compare-profile-data)
         (juxt
          :mean-total-time-increase-over-base
          :mean-comparison-time-per-call)
         (juxt
          :total-time-increase-per-endpoint-call-over-base
          :total-time-per-endpoint-call))]
    (write-to-json-file
     (sort-methods-in-endpoints
      base-profile-data
      :total-time
      :mean-total-time-per-call)
     (str output-dir "/base-aggregated-profile.json"))
    (write-to-json-file
     (sort-methods-in-endpoints
      compare-profile-data
      :total-time
      :mean-total-time-per-call)
     (str output-dir "/compare-aggregated-profile.json"))
    (write-to-json-file
     (flip-endpoint-base-and-comparison-elements
      base-greater-than-compare-data)
     (str output-dir "/base-greater-aggregated-profile.json"))
    (write-to-flat-file
     base-greater-than-compare-data
     (str output-dir "/base-greater-aggregated-profile.txt")
     true)
    (write-to-json-file
     compare-greater-than-base-data
     (str output-dir "/compare-greater-aggregated-profile.json"))
    (write-to-flat-file
     compare-greater-than-base-data
     (str output-dir "/compare-greater-aggregated-profile.txt")
     false)))

(defn canonicalized-path
  [raw-path]
  (-> raw-path fs/file (.getCanonicalPath)))

(def cli-options
  [["-a" "--load-average-info-from-files"
    "Load the endpoint average info from json files"
    :id :load-average-info-from-files
    :default false]
   ["-o" "--output-dir OUTPUT_DIR" "Output directory"
    :id :output-dir
    :default "."
    :parse-fn canonicalized-path
    :validate [fs/readable? "Output directory must be readable"]]
   ["-c" "--compare-dir-or-file COMPARE_DIR_OR_FILE"
    "Compare directory with json files or an individual file"
    :id :compare-dir-or-file
    :default nil
    :parse-fn canonicalized-path
    :validate [fs/readable? "Compare directory must be readable"]]
   ["-f" "--filter FILTER_FILE_EXPRESSION"
    "Filter out any json files matching the supplied expression"
    :id :file-filter
    :default nil
    :parse-fn identity]
   ["-h" "--help"]])

(defn usage [options-summary]
  (->> ["Aggregate info from jruby profile json files."
        ""
        "Usage: jruby-profile-aggregator [options] base-dir-or-file"
        ""
        "Options:"
        options-summary
        ""
        "base-dir: Base directory with json files or individual json file"]
       (string/join \newline)))

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (string/join \newline errors)))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn average-endpoint-info
  [load-average-info-from-files? dir-or-file file-filter]
  (if load-average-info-from-files?
    (do
      (if-not (fs/file? dir-or-file)
        (throw (IllegalArgumentException.
                (str "Value must be a file: " dir-or-file))))
      (load-average-endpoint-info-from-json-file
       dir-or-file))
    (do
      (if-not (fs/directory? dir-or-file)
        (throw (IllegalArgumentException.
                (str "Value must be a directory: " dir-or-file))))
      (average-endpoint-info-for-dir dir-or-file file-filter))))

(defn -main
  [& args]
  (let [{:keys [options arguments errors summary]}
        (cli/parse-opts args cli-options)]
    (cond
      (:help options) (exit 0 (usage summary))
      (not= (count arguments) 1) (exit 1 (usage summary))
      errors (exit 1 (error-msg errors)))
    (let [base-dir-or-file (-> arguments first canonicalized-path)
          load-average-info-from-files? (:load-average-info-from-files options)
          output-dir (:output-dir options)
          file-filter (:file-filter options)]
      (if-let [compare-dir-or-file (:compare-dir-or-file options)]
        (do
          (println "Comparing two profiles...")
          (let [base-profile-data
                (average-endpoint-info
                 load-average-info-from-files?
                 base-dir-or-file
                 file-filter)
                compare-profile-data
                (average-endpoint-info
                 load-average-info-from-files?
                 compare-dir-or-file
                 file-filter)]
            (compare-profile-dirs base-profile-data
                                  compare-profile-data
                                  output-dir))
          (printf "Wrote files to: %s\n" output-dir))
        (do
          (printf "Processing a single profile: %s...\n"
                  base-dir-or-file)
          (printf "Wrote file: %s\n"
                  (process-one-profile-dir base-dir-or-file
                                           output-dir
                                           file-filter)))))))
