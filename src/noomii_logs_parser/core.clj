(ns noomii-logs-parser.core
  (:use [clojure.contrib.duck-streams :only (read-lines)])
  (:require [clojure.contrib.string :as string ]
            [redis.core :as redis]
            [clojure.contrib.pprint :as printer :only (pprint)]))

; Utility functions
;
(defn parse-integer [str]
    (try (Integer/parseInt str)
         (catch NumberFormatException nfe 0)))

(defn parameters-directory-line? [str]
  (string/substring? "directory_uri" str))

(defn get-uri-from-string [str]
  (second
    (re-find #"\"directory_uri\"=>\"((\w+-?_?'?)+)" str)))

(defn parse-log [log-name]
      (filter parameters-directory-line?
        (read-lines log-name)))


;/home/vagrant/production.log.2
(defn write-keys-to-redis [file]
  (redis/with-server
    {:host "127.0.0.1" :port 6379 :db 0}
    (doseq [uri (parse-log file)]
      (redis/incr (get-uri-from-string uri)))))

(defn create-map-from-redis [key]
  (redis/with-server
    {:host "127.0.0.1" :port 6379 :db 0}
    (doall
      (loop [uri-map {} reminder (redis/keys (str key "*"))]
        (let [uri-key (first reminder)]
        (if (empty? reminder) uri-map
            (recur (assoc
                     uri-map uri-key (parse-integer (redis/get uri-key)))
                   (rest reminder))))))))

; fucntions to manipulate the generated map
(defn sum-requests-in-map [uri-map]
  (reduce #(+  %1 (second %2)) 0 uri-map))

(defn sorted-uri-map [uri-map]
  (into
    (sorted-map-by
      (fn [key1 key2] (<= (get uri-map key2) (get uri-map key1))))
      uri-map))
; These  functions are  for debuggin purposes
;
(defn redis-iterator [list index]
        (cond
          (empty? list) "Im done"
        :else (do
                (println index (get-uri-from-string (first list)))
                (redis/incr (get-uri-from-string (first list)))
                 (recur (rest list) (+ 1 index)))))

(defn write-keys-to-redis-2 [file]
  (redis/with-server
    {:host "127.0.0.1" :port 6379 :db 0}
    (do
      (redis-iterator (parse-log "/home/vagrant/production.log.bck") 0))))


;"  Parameters: {\"action\"=>\"index\", \"controller\"=>\"redesign/coaches\",
;\"directory_uri\"=>\"health-and-fitness-coach-denver-colorado&ved=1t:1527,r:10,s:0\"}
;
