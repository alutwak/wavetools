(defpackage :wavetools
  (:nicknames :wt)
  (:use :cl)
  #+sbcl (:lock t)
  (:export
           ;; wavetable
           :spectral-point
           :spectral-point-ts
           :spectral-point-c
           :spectral-point-a1
           :spectral-point-a2
           :spectral-point-r1
           :spectral-point-r2
           :spectral-point-fsep
           :make-spect-point
           
           :spreading-function
           :directional-spectrum
           :spectrum

           ;; station
           :station
           :id
           :metadata
           :data
           :end
           :source
           :cdip

           :station-metadata
           :make-station-metadata
           
           :freqs
           :freqs-defined
           :metadata-defined
           :spoint-size
           :lat
           :lon
           :depth
           :get-spectrum
           :station-push
           :begin
           :clear-data

           ;; station-cache
           :write-station
           :with-cache-writer
           :read-cache-station
           :read-cache-data
           :read-cache

           ;; station-download
           :download-station-data
           :download-station-metadata
           :download-station))
