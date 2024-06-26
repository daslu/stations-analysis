(ns gtfs
  (:require [tablecloth.api :as tc]
            [tablecloth.column.api :as tcc]
            [tech.v3.datatype.functional :as fun]
            [scicloj.noj.v1.vis.hanami :as hanami]
            [scicloj.noj.v1.vis.stats :as vis.stats]
            [aerial.hanami.templates :as ht]
            [clojure2d.color :as color]
            [scicloj.kindly.v4.kind :as kind]
            [fastmath.clustering :as clustering]
            [fastmath.core :as fastmath]
            [fastmath.stats :as stats]
            [charred.api :as charred]
            [clojure.reflect :as reflect]
            [tech.v3.dataset.print :as print]
            [geo
             [geohash :as geohash]
             [jts :as jts]
             [spatial :as spatial]
             [io :as geoio]
             [crs :as crs]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [tech.parallel :as parallel]
            [clojure.math :as math])
  (:import (org.jgrapht.graph SimpleGraph SimpleWeightedGraph
                              SimpleDirectedWeightedGraph
                              AsUndirectedGraph
                              DefaultEdge DefaultWeightedEdge)
           (org.jgrapht.alg.scoring BetweennessCentrality
                                    ClosenessCentrality)
           org.jgrapht.alg.shortestpath.DijkstraShortestPath
           (org.locationtech.jts.index.strtree STRtree)
           (org.locationtech.jts.geom Geometry Point Polygon Coordinate)
           (org.locationtech.jts.geom.prep PreparedGeometry
                                           PreparedLineString
                                           PreparedPolygon
                                           PreparedGeometryFactory)
           org.jgrapht.alg.util.Pair))



(set! *warn-on-reflection* true)



(def WGS84 (geo.crs/create-crs 4326))
;; https://epsg.io/?q=Israel
(def Israel1993 (geo.crs/create-crs 2039))

(def crs-transform-Israel1993->WGS84
  (geo.crs/create-transform Israel1993 WGS84))

(def crs-transform-WGS84->Israel1993
  (geo.crs/create-transform WGS84 Israel1993))

(defn Israel1993->WGS84 [geometry]
  (geo.jts/transform-geom geometry crs-transform-Israel1993->WGS84))

(defn WGS84->Israel1993 [geometry]
  (geo.jts/transform-geom geometry crs-transform-WGS84->Israel1993))

(defn yx->point [y x]
  (jts/point (jts/coordinate y x)))

(defn point->yx [^Point point]
  (let [c (.getCoordinate point)]
    [(.getY c)
     (.getX c)]))



(defn slurp-gzip
  "Read a gzipped file into a string"
  [path]
  (with-open [in (java.util.zip.GZIPInputStream. (clojure.java.io/input-stream path))]
    (slurp in)))


(defonce statistical-areas
  (-> "data/statistical_areas/statistical_areas_2022.geojson.gz"
      slurp-gzip
      geoio/read-geojson))

(def statistical-areas-with-xy
  (->> statistical-areas
       (map #(update % :geometry WGS84->Israel1993))))

(def sa->statistical-area
  (->> statistical-areas
       (map (fn [statistical-area]
              (let [{:keys [SEMEL_YISHUV STAT_2022]} (:properties statistical-area)]
                [[SEMEL_YISHUV STAT_2022] statistical-area])))
       (into {})))


(defn make-spatial-index [rows]
  (let [tree (STRtree.)]
    (doseq [{:keys [^Geometry geometry]
             :as row} rows]
      (.insert tree
               (.getEnvelopeInternal geometry)
               (assoc row
                      :prepared-geometry
                      (org.locationtech.jts.geom.prep.PreparedGeometryFactory/prepare geometry))))
    tree))


(def statistical-areas-index
  (make-spatial-index statistical-areas-with-xy))

(defn yx->statistical-areas [yx]
  (let [^Point point (apply yx->point (reverse yx))]
    (.query ^STRtree statistical-areas-index (.getEnvelopeInternal (.buffer point 100)))))

(defn yx->yishuv [yx]
  (->> yx
       yx->statistical-areas
       (map (comp (juxt :SEMEL_YISHUV :SHEM_YISHUV) :properties))
       set))

(defn yx->sa [yx]
  (->> yx
       yx->statistical-areas
       (map (comp (juxt :SEMEL_YISHUV :STAT_2022) :properties))
       set))

(def dankal-geojson
  (-> "data/dankal/LRT_STAT.json.gz"
      slurp-gzip
      (charred/read-json {:key-fn keyword})))


#_(->> dankal-geojson
       :features
       (map (fn [feature]
              (let [line (-> feature
                             :properties
                             :description
                             (#(cond (re-find #"<td>אדום</td>" %) "דנקל - אדום"
                                     (re-find #"<td>סגול</td>" %) "דנקל - סגול"
                                     :else nil)))]
                {:line line
                 :name (-> feature :properties :Name)}))))


(def dankal-stops
  (->> dankal-geojson
       :features
       (map-indexed (fn [i feature]
                      (let [line (-> feature
                                     :properties
                                     :description
                                     (#(cond (re-find #"<td>אדום</td>" %) "דנקל - אדום"
                                             (re-find #"<td>סגול</td>" %) "דנקל - סגול"
                                             :else nil)))]
                        {:stop_id (+ 1000000 i)
                         :line line
                         :stop_name (->> feature
                                         :properties
                                         :Name
                                         (str line " - "))
                         :stop_lat (-> feature
                                       :geometry
                                       :coordinates
                                       second)
                         :stop_lon (-> feature
                                       :geometry
                                       :coordinates
                                       first)})))
       (filter :line)
       tc/dataset))

(def dankal-name->stop
  (-> dankal-stops
      (tc/rows :as-maps)
      (->> (filter :line)
           (map (juxt :stop_name :stop_id))
           (into {}))))

(defn seq->pairs [s]
  (map vector
       s
       (rest s)))

(defn seq->double-pairs [s]
  (->> s
       seq->pairs
       (mapcat (juxt identity reverse))))

#_(-> dankal-stops
      (tc/select-rows #(-> % :line (= "דנקל - אדום")))
      :stop_name
      vec)

(def dankal-edges
  (->> [["דנקל - סגול - הטייסים"
         "דנקל - סגול - אלטלף"
         "דנקל - סגול - ויצמן"
         "דנקל - סגול - יהוד מרכז"
         "דנקל - סגול - יהוד מערב"
         "דנקל - סגול - סביונים"
         "דנקל - סגול - מונוסון"
         "דנקל - סגול - צומת סביון"
         "דנקל - סגול - נוה סביון"
         "דנקל - סגול - יגאל אלון"
         "דנקל - סגול - קזז"
         "דנקל - סגול - רמת פנקס"
         "דנקל - סגול - מסובים"
         "דנקל - סגול - אפעל דרום"
         "דנקל - סגול - שיבא"
         "דנקל - סגול - אפעל צפון"
         "דנקל - סגול - אלוף שדה"
         "דנקל - סגול - הירדן"
         "דנקל - סגול - רזיאל"
         "דנקל - סגול - כורזין"
         "דנקל - סגול - המאבק"
         "דנקל - סגול - פארק וולפסון"
         "דנקל - סגול - משה דיין"
         "דנקל - סגול - גבעתי"
         "דנקל - סגול - האצ\"ל"
         "דנקל - סגול - ההגנה"
         "דנקל - סגול - נוה שאנן"
         "דנקל - סגול - הר ציון"
         "דנקל - סגול - העליה"
         "דנקל - סגול - יהודה הלוי"
         "דנקל - סגול - מונטיפיורי"
         "דנקל - סגול - הכרמל"
         "דנקל - סגול - מוגרבי"
         "דנקל - סגול - בוגרשוב"
         "דנקל - סגול - גורדון"
         "דנקל - סגול - דיזנגוף"
         "דנקל - סגול - אבן גבירול"
         "דנקל - סגול - איכילוב"
         "דנקל - סגול - מסוף ארלוזורוב"]
        ["דנקל - סגול - בר אילן מזרח"
         "דנקל - סגול - בר אילן מערב"
         "דנקל - סגול - גני אילן"
         "דנקל - סגול - גורן"
         "דנקל - סגול - אפעל צפון"]
        ["דנקל - אדום - הקוממיות"
         "דנקל - אדום - העמל"
         "דנקל - אדום - כ\"ט בנובמבר"
         "דנקל - אדום - יוספטל"
         "דנקל - אדום - בנימין"
         "דנקל - אדום - בלפור"
         "דנקל - אדום - זבוטנסקי"
         "דנקל - אדום - רוטשילד"
         "דנקל - אדום - העצמאות"
         "דנקל - אדום - מחרוזת"
         "דנקל - אדום - הבעש\"ט"
         "דנקל - אדום - איסקוב"
         "דנקל - אדום - ארליך"
         "דנקל - אדום - בן צבי"
         "דנקל - אדום - סלמה"
         "דנקל - אדום - אליפלט"
         "דנקל - אדום - אלנבי"
         "דנקל - אדום - קרליבך"
         "דנקל - אדום - יהודית"
         "דנקל - אדום - שאול המלך"
         "דנקל - אדום - ארלוזורוב"
         "דנקל - אדום - אבא הלל"
         "דנקל - אדום - ביאליק"
         "דנקל - אדום - בן גוריון"
         "דנקל - אדום - אהרונוביץ"
         "דנקל - אדום - שנקר"
         "דנקל - אדום - שחם"
         "דנקל - אדום - בילינסון"
         "דנקל - אדום - דנקנר"
         "דנקל - אדום - קרול"
         "דנקל - אדום - פינסקר"
         "דנקל - אדום - תחנה מרכזית פתח"]
        ["דנקל - אדום - אהרונוביץ"
         "דנקל - אדום - אם המושבות"
         "דנקל - אדום - קרית אריה"]]
       (map (partial map dankal-name->stop))
       (mapcat seq->double-pairs)))

(def base-path
  "data/gtfs-static/public-transportation/")

(defonce bus-stops
  (-> base-path
      (str "stops.csv.gz")
      (tc/dataset {:key-fn keyword})))

(def all-stops
  (tc/concat bus-stops
             dankal-stops))


(def stops-with-location
  (-> all-stops
      (tc/map-columns :WGS84 [:stop_lon :stop_lat] yx->point)
      (tc/map-columns :Israel1993 [:WGS84] WGS84->Israel1993)
      (tc/map-columns :yx [:Israel1993] point->yx)
      (tc/map-columns :y [:yx] first)
      (tc/map-columns :x [:yx] second)
      (tc/map-columns :sa [:yx] yx->sa)
      (tc/map-columns :yishuv [:yx] yx->yishuv)))

(def our-cities
  #{681
    2400
    2620
    5000
    6100
    6200
    8600
    7900
    9400})

(def stops-in-region
  (-> stops-with-location
      (tc/select-rows (fn [{:keys [yishuv]}]
                        (->> yishuv
                             (map first)
                             (some our-cities))))))


(def stop-ids-in-region
  (-> stops-in-region
      :stop_id
      set))

(delay
  (-> stops-in-region
      :yishuv
      (->> (mapcat (partial map first))
           frequencies
           (sort-by second)
           reverse)))


(delay
  (-> stops-in-region
      :yishuv
      (->> (mapcat (partial map second))
           frequencies
           (sort-by second)
           reverse)))

(defonce bus-stop-times
  (-> base-path
      (str "stop_times.csv.gz")
      (tc/dataset {:key-fn keyword})))

(comment
  (-> bus-stop-times
      (print/print-range :all)
      (tc/head 1000)))


(def relevant-stop-times
  (-> bus-stop-times
      (tc/group-by :trip_id)
      (tc/without-grouping->
       (tc/select-rows
        (fn [row]
          (->> row
               :data
               :stop_id
               (some stop-ids-in-region)))))
      (tc/order-by [:stop_sequence])
      (tc/ungroup)
      time))


(tc/row-count bus-stop-times)
(tc/row-count relevant-stop-times)

(def relevant-stop-ids
  (-> relevant-stop-times
      :stop_id
      (concat stop-ids-in-region)
      set))




(count stop-ids-in-region)
(count relevant-stop-ids)

(def relevant-stops
  (-> stops-with-location
      (tc/select-rows #(-> % :stop_id relevant-stop-ids))))

(delay
  (-> relevant-stops
      :line
      frequencies))


(def distance-based-edges
  (memoize (fn [radius]
             (let [rows (-> relevant-stops
                            (tc/rows :as-maps))]
               (->> rows
                    vec
                    (parallel/queued-pmap
                     32
                     (fn [row0]
                       (->> rows
                            (into
                             []
                             (comp (filter (fn [row1]
                                             (and
                                              (< (:stop_id row0)
                                                 (:stop_id row1))
                                              (< (fastmath/dist (:yx row0)
                                                                (:yx row1)) radius))))
                                   (map (fn [row1]
                                          [(:stop_id row0)
                                           (:stop_id row1)])))))))
                    (apply concat)
                    (mapcat (juxt identity reverse)))))))

(delay
  (->> [0 1 100 250]
       (map distance-based-edges)
       (mapv count)
       time))


(def bus-edges
  (-> relevant-stop-times
      (tc/group-by [:trip_id] {:result-type :as-seq})
      (->> (mapcat (fn [{:keys [stop_id]}]
                     (seq->pairs stop_id)))
           (filter (partial apply not=))
           distinct)))

(count bus-edges)

(delay
  (->> bus-edges
       (apply concat)
       (map (comp some? stop-ids-in-region))
       frequencies))

(delay
  (->> bus-edges
       (apply concat)
       (map (comp some? relevant-stop-ids))
       frequencies))


(def vertices
  (memoize (fn [{:keys [dankal-lines]}]
             (-> relevant-stops
                 (tc/select-rows (fn [{:keys [line]}]
                                   (if line
                                     (dankal-lines line)
                                     true)))
                 :stop_id
                 set))))


(defn weighted-edge [w] (fn [e] [w e]))


(def edges
  (memoize (fn [{:as dankal-spec
                 :keys [dankal-lines dist-weight dankal-weight]}]
             (let [vs (vertices dankal-spec)]
               (-> (concat (map (weighted-edge dist-weight)
                                (distance-based-edges 250))
                           (map (weighted-edge 1)
                                bus-edges)
                           (map (weighted-edge dankal-weight)
                                dankal-edges))
                   (->> (group-by second))
                   (update-vals (fn [edges]
                                  (->> edges
                                       (sort-by first)
                                       first)))
                   vals
                   (->> (filter (fn [[w [v0 v1]]]
                                  (and (vs v0)
                                       (vs v1))))))))))

(count (vertices {:dankal-lines #{}
                  :dankal-weight 0.5
                  :dist-weight 2}))
(count (edges {:dankal-lines #{}
               :dankal-weight 0.5
               :dist-weight 2}))
;; (count (vertices #{"דנקל - אדום"}))
;; (count (edges #{"דנקל - אדום"} 2 0.5))
;; (count (vertices #{"דנקל - אדום" "דנקל - סגול"}))
;; (count (edges #{"דנקל - אדום" "דנקל - סגול"} 2 0.5))


(delay
  (->> (edges #{"דנקל - אדום" "דנקל - סגול"} 0.5)
       (map (fn [[_ vs]]
              [(some? (some #(< % 1000000) vs))
               (some? (some #(>= % 1000000) vs))]))
       frequencies))


(def relevant-stops-lat-lon
  (-> relevant-stops
      (tc/select-columns [:stop_id :stop_lat :stop_lon :yx :stop_name :line])
      (tc/set-dataset-name nil)))


(defn edges-details [dankal-spec]
  (-> dankal-spec
      edges
      (->> (map (fn [[w [v0 v1]]]
                  {:w w
                   :v0 v0
                   :v1 v1})))
      tc/dataset
      (tc/left-join relevant-stops-lat-lon {:left :v0 :right :stop_id})
      (tc/left-join relevant-stops-lat-lon {:left :v1 :right :stop_id})
      (tc/map-columns :yx-distance [:yx :.yx] fastmath/dist)
      (tc/select-columns [:stop_lat :.stop_lat
                          :stop_lon :.stop_lon
                          :stop_id :.stop_id
                          :stop_name :.stop_name
                          :line :.line
                          :w :yx-distance])
      (tc/rename-columns {:stop_lat :stop_lat0
                          :stop_lon :stop_lon0
                          :.stop_lat :stop_lat1
                          :.stop_lon :stop_lon1
                          :stop_id :stop_id0
                          :.stop_id :stop_id1
                          :stop_name :stop_name0
                          :.stop_name :stop_name1
                          :line :line0
                          :.line :line1})))


(delay (edges-details {:dankal-lines #{"דנקל - אדום" "דנקל - סגול"}
                       :dankal-weight 0.5
                       :dist-weight 2}))


(delay
  (-> #{}
      (edges-details 0.5)
      (tc/select-rows #(-> % :w (= 1)))
      (vis.stats/histogram :yx-distance {:nbins 100})))


(delay
  (-> #{}
      edges-details
      (tc/select-rows #(and (-> % :w (= 1))
                            (-> % :yx-distance (> 2000))))))

(delay
  (-> #{}
      edges-details
      (tc/select-rows #(-> % :stop_id0 (= 13807)))))

(delay
  (-> bus-stop-times
      (tc/group-by [:trip_id] {:result-type :as-seq})
      (->> (filter (fn [ds]
                     (->> ds
                          :stop_id
                          (filter #{13192 29521})
                          count
                          (= 2))))
           (filter (fn [ds]
                     (-> ds
                         (tc/select-rows
                          #(-> % :stop_id (#{13192 29521})))
                         :stop_sequence
                         (= [34 36]))))
           (map #(print/print-range % :all)))))


(delay
  (-> relevant-stop-times
      (tc/group-by [:trip_id] {:result-type :as-seq})
      (->> (filter (fn [ds]
                     (->> ds
                          :stop_id
                          (filter #{13192 29521})
                          count
                          (= 2))))
           (filter (fn [ds]
                     (-> ds
                         (tc/select-rows
                          #(-> % :stop_id (#{13192 29521})))
                         :stop_sequence
                         (= [34 36]))))
           (map (fn [ds]
                  (-> ds
                      (tc/left-join bus-stops [:stop_id])
                      (tc/order-by [:stop_sequence]))))
           ;; (map (fn [{:keys [stop_id]}]
           ;;        [(vec stop_id)
           ;;         (seq->pairs stop_id)]))
           )
      kind/fragment))


(delay
  (-> relevant-stop-times
      (tc/select-rows #(-> % :trip_id (= "58029926_020524")))
      (tc/left-join bus-stops :stop_id)
      (print/print-range :all)))


(delay (->> bus-edges
            (filter #(= % [13192 29521]))))

(def graph
  (memoize (fn [dankal-spec]
             (let [g (SimpleDirectedWeightedGraph. DefaultWeightedEdge)]
               (doseq [v (vertices dankal-spec)]
                 (.addVertex g v))
               (doseq [[w [v0 v1]] (edges dankal-spec)]
                 (let [e (.addEdge g v1 v0)]
                   (.setEdgeWeight
                    g
                    e
                    w)))
               g))))


(delay
  (-> #{}
      edges
      (->> (map (fn [[w [v0 v1]]]
                  {:w w
                   :v0 v0
                   :v1 v1})))
      tc/dataset
      (tc/left-join relevant-stops-lat-lon {:left :v0 :right :stop_id})
      (tc/left-join relevant-stops-lat-lon {:left :v1 :right :stop_id})))



;; 13320 Azrieli
;; 50379 Petah Tikva Merkazit
;; 42872 Savidor

(def stop-id->name (-> relevant-stops
                       (tc/select-columns [:stop_id :stop_name])
                       tc/rows
                       (->> (into {}))))


(delay
  (->> [[50379 42872]
        [50379 13947]]
       (map (fn [[source-id target-id]]
              (let [
                    predecessors (-> {:dankal-lines #{}
                                      :dankal-weight 1
                                      :dist-weight 1}
                                     graph
                                     (DijkstraShortestPath.)
                                     (.getPaths source-id)
                                     (.getDistanceAndPredecessorMap)
                                     (->> (into {}))
                                     (update-vals (fn [^Pair p]
                                                    [(.getFirst p)
                                                     (-> p
                                                         (.getSecond)
                                                         str
                                                         (str/replace #"[\(| |\)]" "")
                                                         (str/split #":")
                                                         (->> (mapv #(try (Integer/parseInt %)
                                                                          (catch Exception e nil)))))])))]
                (-> (loop [v target-id
                           story []]
                      (let [[distance [next-v _]] (predecessors v)]
                        (if (< distance 1)
                          story
                          (recur next-v
                                 (conj story {;; :stop_id v
                                              :stop-name (stop-id->name v)
                                              ;; :distance distance
                                              })))))
                    vec
                    (conj {;; :stop_id source-id
                           :stop-name (stop-id->name source-id)})
                    tc/dataset))))
       kind/fragment))






(def score
  (memoize
   (fn [dankal-spec target-stop-id]
     (-> dankal-spec
         graph
         (DijkstraShortestPath.)
         (.getPaths target-stop-id)
         (.getDistanceAndPredecessorMap)
         (->> (into {}))
         (update-vals (fn [^Pair pair]
                        (.getFirst pair)))))))




(delay
  (-> (->> [#{}
            #{"דנקל - אדום" "דנקל - סגול"}]
           (map (fn [dankal-lines]
                  (-> {:dankal-lines dankal-lines
                       :dankal-weight 0.5
                       :dist-weight 2}
                      (score 50379)
                      (->> (map (fn [[stop_id score]]
                                  {:stop_id stop_id
                                   :score score})))
                      tc/dataset)))
           (apply #(tc/left-join %1 %2 [:stop_id])))
      (tc/map-columns :eq [:score :right.score] =)
      :eq
      frequencies))


(def betweeness
  (memoize
   (fn [dankal-spec]
     (-> dankal-spec
         graph
         (BetweennessCentrality. true)
         .getScores
         (->> (into {}))
         time))))

(def closeness
  (memoize
   (fn [dankal-spec]
     (-> dankal-spec
         graph
         (ClosenessCentrality.)
         .getScores
         (->> (into {}))
         time))))



(delay
  (-> (->> [#{} #{"דנקל - אדום" "דנקל - סגול"}]
           (map (fn [dankal-lines]
                  (-> {:dankal-lines dankal-lines
                       :dankal-weight 0.5
                       :dist-weight 2}
                      (score 50379)
                      (->> (map (fn [[stop_id score]]
                                  {:stop_id stop_id
                                   :score score})))
                      tc/dataset)))
           (apply #(tc/left-join %1 %2 [:stop_id])))
      (tc/map-columns :eq [:score :right.score] =)
      :eq
      frequencies))


(def scored-stops
  (memoize (fn [dankal-spec target-stop-id]
             (case target-stop-id
               :betweeness (-> relevant-stops
                               (tc/map-columns :betweeness [:stop_id] (betweeness dankal-spec)))
               :closeness (-> relevant-stops
                              (tc/map-columns :closeness [:stop_id] (closeness dankal-spec)))
               (-> relevant-stops
                   (tc/map-columns :score [:stop_id] (score dankal-spec target-stop-id)))))))

(delay
  (-> {:dankal-lines #{"דנקל - אדום" "דנקל - סגול"}
       :dankal-weight 1
       :dist-weight 1}
      (scored-stops :betweeness)
      (vis.stats/histogram :betweeness {:nbins 100})))


(delay
  (-> {:dankal-lines #{"דנקל - אדום" "דנקל - סגול"}
       :dankal-weight 1
       :dist-weight 1}
      (scored-stops :closeness)
      :closeness
      frequencies))

(delay
  (-> #{"דנקל - אדום" "דנקל - סגול"}
      (scored-stops 50379)
      (tc/select-rows #(-> % :betweeness pos?))
      (vis.stats/histogram :log-betweeness {:nbins 100})))


(delay
  (-> {:dankal-lines #{"דנקל - אדום" "דנקל - סגול"}
       :dankal-weight 0.5
       :dist-weight 2}
      (scored-stops 50379)
      (tc/group-by [:score])
      (tc/aggregate {:n tc/row-count})
      (hanami/plot ht/bar-chart {:X :score
                                 :Y :n})))

(defn as-geo [vega-lite-spec]
  (-> vega-lite-spec
      (update :encoding update-keys
              #(case %
                 :x :latitude
                 :y :longitude
                 :x2 :latitude2
                 :y2 :longitude2
                 %))
      (assoc :projection {:type :mercator})))


(delay
  (-> {:dankal-lines #{"דנקל - אדום" "דנקל - סגול"}
       :dankal-weight 0.5
       :dist-weight 1}
      (scored-stops 50379)
      (tc/map-columns :close [:score] #(some-> % (< 6)))
      (hanami/plot ht/point-chart
                   {:X "stop_lat"
                    :Y "stop_lon"
                    :COLOR "close"
                    :OPACITY 0.1})
      as-geo))

(defn leaflet-map [geojson]
  (kind/reagent
   ['(fn [{:keys [provider
                  center
                  geojson]}]
       (.log js/console geojson)
       [:div
        {:style {:height "600px"}
         :ref   (fn [el]
                  (let [m (-> js/L
                              (.map el)
                              (.setView (clj->js center)
                                        12))]
                    (-> js/L
                        .-tileLayer
                        (.provider provider)
                        (.addTo m))
                    (-> js/L
                        (.geoJson (clj->js geojson)
                                  ;; https://gist.github.com/geog4046instructor/ac36ea8f317912f51f0bb1d50b5c3481
                                  (clj->js {:pointToLayer (fn [feature latlng]
                                                            (-> js/L
                                                                (.circle latlng
                                                                         (-> feature
                                                                             .-properties
                                                                             .-style))))
                                            :style (fn [feature]
                                                     (-> feature
                                                         .-properties
                                                         .-style))}))
                        (.bindTooltip (fn [layer]
                                        (-> layer
                                            .-feature
                                            .-properties
                                            .-tooltip)))
                        (.addTo m))))}])
    {:provider "Stadia.AlidadeSmooth"
     :center   [32.075 34.8]
     :geojson geojson}]
   {:reagent/deps [:leaflet]}))


(def grad
  (color/gradient [:red :yellow]))



(defn stops-map [dankal-spec target-stop-id]
  (leaflet-map
   {:type :FeatureCollection
    :features
    (vec
     (concat (-> dankal-spec
                 (scored-stops target-stop-id)
                 (tc/rows :as-maps)
                 (->> (mapv (fn [{:keys [stop_id stop_name stop_lat stop_lon score]}]
                              (let [radius 50
                                    color (or (some-> score
                                                      (/ 20)
                                                      grad
                                                      color/format-hex)
                                              "black")]
                                {:type :Feature
                                 :geometry {:type :Point
                                            :coordinates [stop_lon stop_lat]}
                                 :properties {:style {:radius radius
                                                      :fillColor color
                                                      :color color
                                                      :weight 1
                                                      :opacity 1
                                                      :fillOpacity 0.5}
                                              :tooltip (str stop_id
                                                            " "
                                                            stop_name
                                                            " "
                                                            (some->> score
                                                                     (format "%.0f")))}})))))
             (-> dankal-spec
                 edges-details
                 (tc/select-rows #(and (-> % :stop_id0 (> 1000000))
                                       (-> % :stop_id1 (> 1000000)))) ; dankal
                 (tc/rows :as-maps)
                 (->> (mapv (fn [{:keys [stop_lat0 stop_lon0 stop_lat1 stop_lon1]}]
                              {:type :Feature
                               :geometry {:type :LineString
                                          :coordinates [[stop_lon0 stop_lat0]
                                                        [stop_lon1 stop_lat1]]}}))))))}))


(let [target-stop-id #_13947 50379]
  (kind/fragment
   [;; (stops-map {:dankal-lines #{"דנקל - אדום" "דנקל - סגול"}
    ;;             :dankal-weight 0.5
    ;;             :dist-weight 2}
    ;;            target-stop-id)
    (stops-map {:dankal-lines #{}
                :dankal-weight 0.5
                :dist-weight 2}
               target-stop-id)]))



(delay
  (let [target-stop-id 48920]
    (-> (->> [[:none #{}]
              [:red #{"דנקל - אדום"}]
              [:red-purple #{"דנקל - אדום" "דנקל - סגול"}]]
             (map (fn [[dankal-lines-id dankal-lines]]
                    (-> {:dankal-lines dankal-lines
                         :dankal-weight 1/3
                         :dist-weight 1}
                        (scored-stops target-stop-id)
                        (tc/rows :as-maps)
                        (->> (mapcat (fn [{:keys [score sa]}]
                                       (->> sa
                                            (map (fn [sa0]
                                                   {:sa sa0
                                                    :score score}))))))
                        tc/dataset
                        (tc/add-column :dankal-lines-id
                                       dankal-lines-id))))
             (apply tc/concat))
        (tc/group-by [:dankal-lines-id :sa])
        (tc/aggregate {:score (fn [ds]
                                (-> ds
                                    :score
                                    fun/mean))})
        (tc/pivot->wider :dankal-lines-id
                         :score)
        (tc/map-columns :deltared [:none :red] (fn [x y]
                                                 (max 0 (- x y))))
        (tc/map-columns :deltapurple [:red :red-purple] (fn [x y]
                                                          (max 0 (- x y))))
        (tc/select-rows #(-> % :deltared (>= 0)))
        (tc/select-rows #(-> % :deltapurple (>= 0)))
        (tc/select-rows #(-> % :sa first our-cities))
        ((juxt #(hanami/plot % ht/point-chart
                             {:X :deltared
                              :Y :deltapurple
                              :XTITLE "תועלת הקו האדום"
                              :YTITLE "תועלת הקו הסגול אחרי האדום"
                              ;; :COLOR "sa"
                              :MSIZE 200
                              :OPACITY 0.5})
               #(-> %
                    (tc/select-rows (fn [row] (-> row :deltared (> 0))))
                    (hanami/plot ht/bar-chart
                                 {:X :deltared
                                  :XBIN true
                                  :YAGG "count"
                                  :XTITLE "תועלת הקו האדום"
                                  :YTITLE "מספר אזורים"})
                    (assoc-in [:encoding :x :bin]
                              {:maxbins 50}))
               #(-> %
                    (tc/select-rows (fn [row] (-> row :deltapurple (> 0))))
                    (hanami/plot ht/bar-chart
                                 {:X :deltapurple
                                  :XBIN true
                                  :YAGG "count"
                                  :XTITLE "תועלת הקו הסגול אחריהאדום"
                                  :YTITLE "מספר אזורים"})
                    (assoc-in [:encoding :x :bin]
                              {:maxbins 50})))))))


(delay
  (let [target-stop-id 48920]
    (-> (->> [[:none #{}]
              [:red #{"דנקל - אדום"}]
              [:red-purple #{"דנקל - אדום" "דנקל - סגול"}]]
             (map (fn [[dankal-lines-id dankal-lines]]
                    (-> {:dankal-lines dankal-lines
                         :dankal-weight 0.5
                         :dist-weight 2}
                        (scored-stops target-stop-id)
                        (tc/rows :as-maps)
                        (->> (mapcat (fn [{:keys [score yishuv]}]
                                       (->> yishuv
                                            (map (fn [y]
                                                   {:yishuv y
                                                    :score score}))))))
                        tc/dataset
                        (tc/add-column :dankal-lines-id
                                       dankal-lines-id))))
             (apply tc/concat))
        (tc/group-by [:dankal-lines-id :yishuv])
        (tc/aggregate {:score (fn [ds]
                                (-> ds
                                    :score
                                    fun/mean))})
        (tc/pivot->wider :dankal-lines-id
                         :score)
        (tc/map-columns :delta-red [:none :red] -)
        (tc/map-columns :delta-purple [:red :red-purple] -)
        (tc/select-rows #(> (+ (:delta-red %)
                               (:delta-purple %))
                            0))
        (tc/select-rows #(-> % :yishuv first our-cities))
        (hanami/plot ht/point-chart
                     {:X :delta-red
                      :Y :delta-purple
                      :COLOR "y"
                      :MSIZE 100}))))







(defn choropleth-map [features]
  (kind/reagent
   ['(fn [{:keys [provider
                  center
                  geojson]}]
       [:div
        {:style {:height "600px"}
         :ref   (fn [el]
                  (let [m (-> js/L
                              (.map el)
                              (.setView (clj->js center)
                                        12))]
                    (-> js/L
                        .-tileLayer
                        (.provider provider)
                        (.addTo m))
                    (-> js/L
                        (.geoJson (clj->js geojson)
                                  (clj->js {:style (fn [feature]
                                                     (-> feature
                                                         .-properties
                                                         .-style))}))
                        (.bindTooltip (fn [layer]
                                        (-> layer
                                            .-feature
                                            .-properties
                                            .-tooltip)))
                        (.addTo m))))}])
    {:provider "Stadia.AlidadeSmooth"
     :center   [32.075 34.8]
     :geojson {:type :FeatureCollection
               :features features}}]
   {:reagent/deps [:leaflet]}))


(defn map-changes [{:keys [target-stop-id
                           dankal-weight
                           dist-weight
                           what-delta]}]
  (-> (->> [[:none #{}]
            [:red #{"דנקל - אדום"}]
            [:red-purple #{"דנקל - אדום" "דנקל - סגול"}]]
           (map (fn [[dankal-lines-id dankal-lines]]
                  (-> {:dankal-lines dankal-lines
                       :dankal-weight dankal-weight
                       :dist-weight dist-weight}
                      (scored-stops target-stop-id)
                      (tc/rows :as-maps)
                      (->> (mapcat (fn [{:keys [score sa]}]
                                     (->> sa
                                          (map (fn [sa0]
                                                 {:sa sa0
                                                  :score score}))))))
                      tc/dataset
                      (tc/add-column :dankal-lines-id
                                     dankal-lines-id))))
           (apply tc/concat))
      (tc/group-by [:dankal-lines-id :sa])
      (tc/aggregate {:score (fn [ds]
                              (-> ds
                                  :score
                                  fun/mean))})
      (tc/pivot->wider :dankal-lines-id
                       :score)
      (tc/map-columns :delta-red [:none :red] -)
      (tc/map-columns :delta-purple [:red :red-purple] -)
      (tc/select-rows #(-> % :sa first our-cities))
      (tc/rows :as-maps)
      (->> (mapv (fn [{:as info
                       :keys [sa delta-red delta-purple]}]
                   (let [signal (case what-delta
                                  :purple (> delta-purple 1)
                                  :red (> delta-red 1))
                         statistical-area (sa->statistical-area sa)
                         {:keys [SHEM_YISHUV STAT_2022]} (:properties statistical-area)]
                     {:type :Feature
                      :geometry (-> statistical-area
                                    :geometry
                                    geoio/to-geojson
                                    (charred/read-json {:key-fn keyword}))
                      :properties {:style {:radius 50
                                           :fillColor (if signal
                                                        "orange"
                                                        "black")
                                           :color "black"
                                           :weight 1
                                           :opacity 0
                                           :fillOpacity (if signal 0.8 0)}
                                   :tooltip ""}}))))
      (#(concat % (-> {:dankal-lines #{"דנקל - אדום" "דנקל - סגול"}
                       :dankal-weight 0.5
                       :dist-weight 1}
                      edges-details
                      (tc/select-rows (fn [row]
                                        (and (:line0 row)
                                             (:line1 row)))) ; dankal
                      (tc/rows :as-maps)
                      (->> (mapv (fn [{:keys [stop_lat0 stop_lon0 stop_lat1 stop_lon1 line0 line1]}]
                                   {:type :Feature
                                    :geometry {:type :LineString
                                               :coordinates [[stop_lon0 stop_lat0]
                                                             [stop_lon1 stop_lat1]]}
                                    :properties {:style {:opacity 0.3
                                                         :color (case line0
                                                                  "דנקל - אדום"
                                                                  "red"
                                                                  "דנקל - סגול"
                                                                  "purple")}
                                                 :tooltip ""}}))))))
      vec
      choropleth-map))


(kind/fragment
 (for [what-delta [:red :purple]
       dw [1 1/2 1/3]]
   (kind/fragment
    [(kind/hiccup
      [:div
       [:p (case what-delta
             :red
             "תועלת מהקו האדום"
             :purple
             "תועלת מהקו הסגול")]
       [:p "משקל לרכבת הקלה:"
        dw]])
     (map-changes
      {:target-stop-id 48920
       :dankal-weight dw
       :dist-weight 1
       :what-delta what-delta})])))




(defn relative-change [x0 x1]
  (if (zero? x0)
    (if (> x1 0.001) 1 0)
    (* 100 (/ (- x1 x0) x0))))



(defn absval [x]
  (if (neg? x)
    (- x)
    x))


(defn betweeness-changes [{:keys [dankal-weight
                                  dist-weight
                                  what-change]}]
  (-> (->> [[:none #{}]
            [:red #{"דנקל - אדום"}]
            [:red-purple #{"דנקל - אדום" "דנקל - סגול"}]]
           (map (fn [[dankal-lines-id dankal-lines]]
                  (-> {:dankal-lines dankal-lines
                       :dankal-weight dankal-weight
                       :dist-weight dist-weight}
                      (scored-stops :betweeness)
                      (tc/add-column :dankal-lines-id
                                     dankal-lines-id))))
           (apply tc/concat))
      (tc/select-columns [:stop_id :stop_lon :stop_lat :stop_name :stop_code :stop_desc :betweeness :dankal-lines-id])
      (tc/pivot->wider :dankal-lines-id
                       :betweeness)
      (tc/map-columns :change-red [:none :red] relative-change)
      (tc/map-columns :change-purple [:red :red-purple] relative-change)
      (tc/map-columns :change-purple-abs [:change-purple] absval)
      (tc/order-by [:change-purple-abs] :desc)))


(delay (-> {:dankal-weight 1
            :dist-weight 1
            :what-change :purple}
           betweeness-changes
           (tc/write! "/tmp/betweeness.csv")))




(delay (-> {:dankal-weight 1
            :dist-weight 1
            :what-change :purple}
           betweeness-changes
           (hanami/plot ht/point-chart
                        {:X :none
                         :Y :red-purple})))



(-> {:dankal-weight 1
     :dist-weight 1
     :what-change :purple}
    betweeness-changes
    :none
    (stats/quantiles (fun/* 0.05 (range 1 20))))




(delay
  (-> {:dankal-weight 1
       :dist-weight 1
       :what-change :purple}
      betweeness-changes
      (tc/select-rows #(-> % :stop_code (= 21379)))
      (tc/rows :as-maps)))


{:stop_id 13044
 :stop_code 21379
 :stop_name "פארק אריאל שרון/דרך לוד",
 :stop_desc "רחוב: דרך לוד עיר: רמת גן רציף:  קומה:"
 :none 0.004583722933093994
 :red 0.004552342975711072
 :red-purple 0.004473806157098106
 :change-red -0.6845954225627795
 :change-purple -1.725195553849912}










(defn map-betweeness-changes [{:keys [dankal-weight
                                      dist-weight
                                      what-delta]}]
  (-> (->> [[:none #{}]
            [:red #{"דנקל - אדום"}]
            [:red-purple #{"דנקל - אדום" "דנקל - סגול"}]]
           (map (fn [[dankal-lines-id dankal-lines]]
                  (-> {:dankal-lines dankal-lines
                       :dankal-weight dankal-weight
                       :dist-weight dist-weight}
                      (scored-stops :betweeness)
                      (tc/add-column :dankal-lines-id
                                     dankal-lines-id))))
           (apply tc/concat))
      (tc/select-columns [:stop_id :stop_name :stop_desc :betweeness :dankal-lines-id])
      (tc/pivot->wider :dankal-lines-id
                       :betweeness)
      (tc/map-columns :change-red [:none :red] relative-change)
      (tc/map-columns :change-purple [:red :red-purple] relative-change)
      (tc/map-columns :change-purple-abs [:change-purple] absval)
      (tc/order-by [:change-purple-abs] :desc)
      (tc/select-rows #(and (-> % :none (> 1/10000))
                            (-> % :change-purple-abs (> 10))))
      (tc/left-join all-stops [:stop_id])
      (tc/rows :as-maps)
      (->> (mapv (fn [{:keys [stop_id stop_name stop_lon stop_lat stop_lon stop_lat change-purple]}]
                   (let [color (if (pos? change-purple)
                                 "brown" "blue")]
                     {:type :Feature
                      :geometry {:type :Point
                                 :coordinates [stop_lon stop_lat]}
                      :properties {:style {:radius 200
                                           :fillColor color
                                           :color color
                                           :weight 1
                                           :opacity 0.5
                                           :fillOpacity 0.5}
                                   :tooltip (str stop_id
                                                 " "
                                                 stop_name
                                                 " "
                                                 (some->> change-purple
                                                          (format "%.02f%%")))}}))))
      (#(concat % (-> {:dankal-lines #{"דנקל - אדום" "דנקל - סגול"}
                       :dankal-weight 1
                       :dist-weight 1}
                      edges-details
                      (tc/select-rows (fn [row]
                                        (and (:line0 row)
                                             (:line1 row)))) ; dankal
                      (tc/rows :as-maps)
                      (->> (mapv (fn [{:keys [stop_lat0 stop_lon0 stop_lat1 stop_lon1 line0 line1]}]
                                   {:type :Feature
                                    :geometry {:type :LineString
                                               :coordinates [[stop_lon0 stop_lat0]
                                                             [stop_lon1 stop_lat1]]}
                                    :properties {:style {:opacity 0.3
                                                         :color (case line0
                                                                  "דנקל - אדום"
                                                                  "red"
                                                                  "דנקל - סגול"
                                                                  "purple")}
                                                 :tooltip ""}}))))))
      vec
      leaflet-map))



(delay
  (-> {:dankal-weight 1
       :dist-weight 1
       :what-change :purple}
      map-betweeness-changes))



(kind/fragment
 (for [target-stop-id [14349 39311 15460
                       49141
                       12903
                       37528
                       13243
                       13206]
       what-delta [:purple]
       dw [1/2]]
   (kind/fragment
    [(kind/hiccup
      [:div
       [:p "יעד: "
        (stop-id->name target-stop-id)]
       ;; [:p (case what-delta
       ;;       :red
       ;;       "תועלת מהקו האדום"
       ;;       :purple
       ;;       "תועלת מהקו הסגול")]
       ;; [:p "משקל לרכבת הקלה:"
       ;;  dw]
       ])
     (map-changes
      {:target-stop-id target-stop-id
       :dankal-weight dw
       :dist-weight 1
       :what-delta what-delta})])))




(let [target-stop-id #_13947 50379]
  (kind/fragment
   [;; (stops-map {:dankal-lines #{"דנקל - אדום" "דנקל - סגול"}
    ;;             :dankal-weight 0.5
    ;;             :dist-weight 2}
    ;;            target-stop-id)
    (stops-map {:dankal-lines #{}
                :dankal-weight 0.5
                :dist-weight 2}
               target-stop-id)]))



;; 14349 - tau
;; 39311 - bar ilan
;; 13351 - shiba
;; 16205 - beilinson
;; 12903 - ichilov
;; 37528 - bursa

;; 13243 - ramat hahayal
;; 13206 - kiryat hamemshala


:bye
