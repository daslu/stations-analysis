(ns dev
  (:require [scicloj.clay.v2.api :as clay]))

(clay/make! {:format [:quarto :html]
             :base-source-path "notebooks"
             :source-path ["index.clj" "gtfs.clj"]
             :base-target-path "docs/generated"
             :clean-up-target-dir true})
