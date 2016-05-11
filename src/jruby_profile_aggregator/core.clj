(ns jruby-profile-aggregator.core
    (:require [clojure.java.io :as io]
     [clojure.string :as string]
     [clojure.tools.cli :as cli]
     [cheshire.core :as cheshire]))

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

(defn sum-endpoint-info-for-dir
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
   (sum-endpoint-info-for-dir dir file-filter)))

(defn filter-for-methods-having-greater-mean-total-time-per-call
  [methods base endpoint-name]
  (reduce
   (fn [acc method]
     (let [method-name (key method)
           mean-time-in-comparison
           (:mean-total-time-per-call (val method))
           mean-time-in-base
           (get-in base [endpoint-name
                         :methods
                         method-name
                         :mean-total-time-per-call])
           mean-total-time-increase-over-base
           (if (nil? mean-time-in-base)
             mean-time-in-comparison
             (- mean-time-in-comparison mean-time-in-base))]
       (if (pos? mean-total-time-increase-over-base)
         (assoc acc
           method-name
           (assoc (val method)
             :mean-total-time-increase-over-base
             mean-total-time-increase-over-base))
         acc)))
   {}
   methods))

(defn filter-for-endpoint-methods-having-greater-mean-total-time-per-call
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
           mean-comparison-time-per-call (/ (:total-time endpoint-val)
                                            (:total-calls endpoint-val))]
       (assoc acc
         endpoint-name
         {:mean-total-time-increase-over-base (- mean-comparison-time-per-call
                                                 mean-base-time-per-call)
          :mean-base-time-per-call mean-base-time-per-call
          :mean-comparison-time-per-call mean-comparison-time-per-call
          :methods (filter-for-methods-having-greater-mean-total-time-per-call
                    (:methods endpoint-val)
                    base
                    endpoint-name)})
       #_(assoc acc
           endpoint-name
           (update (val endpoint)
                   :methods
                   filter-for-methods-having-greater-mean-total-time-per-call
                   base
                   endpoint-name))))
   {}
   comparison))

(defn sort-methods-by-mean-total-time-per-call
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

(defn sort-methods-in-endpoints-by-mean-total-time-per-call
  [endpoints endpoint-sort-keys method-sort-keys]
  (sort-by endpoint-sort-keys
           (fn [key1 key2]
             (compare key2 key1))
           (mapv
            (fn [endpoint]
              (->
               (update (val endpoint)
                       :methods
                       sort-methods-by-mean-total-time-per-call
                       method-sort-keys)
               (assoc
                 :name
                 (key endpoint))))
            endpoints))

  #_(reduce
   (fn [acc endpoint]
     (assoc acc (key endpoint)
                (update (val endpoint)
                        :methods
                        sort-methods-by-mean-total-time-per-call
                        method-sort-keys)))
   {}
   endpoints))

(defn write-to-json-file
  [profile-data output-file]
  (cheshire/generate-stream
   profile-data
   (clojure.java.io/writer output-file)))

(defn write-to-flat-file
  [profile output-file]
  (with-open [wrtr (io/writer output-file)]
    (.write wrtr (apply str (repeat 35 "-")))
    (.newLine wrtr)
    (.write wrtr "endpoint summary")
    (.newLine wrtr)
    (.newLine wrtr)
    (.write wrtr (string/join "  "
                              ["mean inc over base"
                               "mean total time per call"
                               "method name"]))
    (.newLine wrtr)
    (.write wrtr (apply str (repeat 70 "-")))
    (.newLine wrtr)
    (doseq [{:keys [mean-total-time-increase-over-base
                    mean-comparison-time-per-call
                    name]} profile]
      (.write wrtr (string/join
                    "  "
                    [
                     (format "%18.6f" mean-total-time-increase-over-base)
                     (format "%24.6f" mean-comparison-time-per-call)
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
                                ["mean inc over base"
                                 "mean total time per call"
                                 "method name"]))
      (.newLine wrtr)
      (.write wrtr (apply str (repeat 70 "-")))
      (.newLine wrtr)
      (doseq [{:keys [mean-total-time-increase-over-base
                      mean-total-time-per-call
                      name]}
              (:methods endpoint)]
        (.write wrtr (string/join
                      "  "
                      [
                       (format "%18.6f" mean-total-time-increase-over-base)
                       (format "%24.6f" mean-total-time-per-call)
                       name]))
        (.newLine wrtr))
      (.newLine wrtr))))

(defn process-one-profile-dir
  [base-dir output-dir file-filter]
  (let [output-file (str output-dir "/one-aggregated-profile.json")]
    (write-to-json-file
     (sort-methods-in-endpoints-by-mean-total-time-per-call
      (average-endpoint-info-for-dir base-dir file-filter)
      :total-time
      :mean-total-time-per-call)
     output-file)
    output-file))

(defn compare-profile-dirs
  [base-dir compare-dir output-dir file-filter]
  (let [base-profile-data
        (average-endpoint-info-for-dir base-dir file-filter)
        compare-profile-data
        (average-endpoint-info-for-dir compare-dir file-filter)
        base-greater-than-compare-data
        (sort-methods-in-endpoints-by-mean-total-time-per-call
         (filter-for-endpoint-methods-having-greater-mean-total-time-per-call
          compare-profile-data
          base-profile-data)
         (juxt
          :mean-total-time-increase-over-base
          :mean-comparison-time-per-call)
         (juxt
          :mean-total-time-increase-over-base
          :mean-total-time-per-call))
        compare-greater-than-base-data
        (sort-methods-in-endpoints-by-mean-total-time-per-call
         (filter-for-endpoint-methods-having-greater-mean-total-time-per-call
          base-profile-data
          compare-profile-data)
         (juxt
          :mean-total-time-increase-over-base
          :mean-comparison-time-per-call)
         (juxt
          :mean-total-time-increase-over-base
          :mean-total-time-per-call))]
    (write-to-json-file
     (sort-methods-in-endpoints-by-mean-total-time-per-call
      base-profile-data
      :total-time
      :mean-total-time-per-call)
     (str output-dir "/base-aggregated-profile.json"))
    (write-to-json-file
     (sort-methods-in-endpoints-by-mean-total-time-per-call
      compare-profile-data
      :total-time
      :mean-total-time-per-call)
     (str output-dir "/compare-aggregated-profile.json"))
    (write-to-json-file
     base-greater-than-compare-data
     (str output-dir "/base-greater-aggregated-profile.json"))
    (write-to-flat-file
     base-greater-than-compare-data
     (str output-dir "/base-greater-aggregated-profile.txt"))
    (write-to-json-file
     compare-greater-than-base-data
     (str output-dir "/compare-greater-aggregated-profile.json"))
    (write-to-flat-file
     compare-greater-than-base-data
     (str output-dir "/compare-greater-aggregated-profile.txt"))))

(def cli-options
  [["-o" "--output-dir OUTPUT_DIR" "Output directory"
    :id :output-dir
    :default "."
    :parse-fn identity]
   ["-c" "--compare-dir COMPARE_DIR" "Compare directory with json files"
    :id :compare-dir
    :default nil
    :parse-fn identity]
   ["-f" "--filter FILTER_FILE_EXPRESSION"
    "Filter out any json files matching the supplied expression"
    :id :file-filter
    :default nil
    :parse-fn identity]
   ["-h" "--help"]])

(defn usage [options-summary]
  (->> ["Aggregate info from jruby profile json files."
        ""
        "Usage: jruby-profile-aggregator [options] base-dir"
        ""
        "Options:"
        options-summary
        ""
        "base-dir: Base directory with json files"]
       (string/join \newline)))

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (string/join \newline errors)))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn -main
  [& args]
  (let [{:keys [options arguments errors summary]}
        (cli/parse-opts args cli-options)]
    (cond
      (:help options) (exit 0 (usage summary))
      (not= (count arguments) 1) (exit 1 (usage summary))
      errors (exit 1 (error-msg errors)))
    (let [base-dir (first arguments)
          output-dir (:output-dir options)
          file-filter (:file-filter options)]
      (if-let [compare-dir (:compare-dir options)]
        (do
          (println "Comparing two profile directories...")
          (compare-profile-dirs base-dir compare-dir output-dir file-filter)
          (printf "Wrote files to: %s\n" output-dir))
        (do
          (printf "Processing a single profile dir: %s...\n"
                  base-dir)
          (printf "Wrote file: %s\n"
                  (process-one-profile-dir base-dir output-dir file-filter)))))))
