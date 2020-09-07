(defpackage :wavetools/station
  (:use :cl :wavetools/wavespectrum)
  (:nicknames :station)
  (:import-from :lisp-binary
                :defbinary
                :counted-array)
  (:export :station
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
           :clear-data))

(in-package :wavetools/station)

(defvar *cdip-stations* '(46267))

(defun init-data-vect ()
  (make-array 100 :fill-pointer 0 :adjustable t))

(defun reset-data-vect (data)
  (setf (fill-pointer data) 0))

;; Defines the metadata for a station
(lisp-binary:defbinary station-metadata ()
  (lat 999.9 :type float)
  (lon 999.9 :type float)
  (depth 0.0 :type float)
  (freqs #() :type (lisp-binary:counted-array 1 float)))

(defclass station ()
  ((id
    :initarg :id
    :accessor id
    :type integer
    :documentation "The id value for the station")
   (metadata
    :initarg :metadata
    :initform (make-station-metadata)
    :accessor metadata
    :type station-metadata
    :documentation "Metadata for the station")
   (data
    :initarg :data
    :initform (init-data-vect)
    :accessor data
    :type vector
    :documentation "Data that is currently loaded for the station")
   (end
    :initform 0
    :reader end
    :type integer
    :documentation "The last time stamp of the data")
   (source
    :reader source
    :documentation "The type of the station ('noaa or 'cdip")))


(defgeneric freqs (station)
  (:documentation "Returns the frequency array for the station"))

(defgeneric (setf freqs) (station freqs)
  (:documentation "Sets the frequency array for the station"))

(defgeneric freqs-defined (station)
  (:documentation "Returns freqs if they have been defined for the station and nil otherwise"))

(defgeneric metadata-defined (station)
  (:documentation "Returns metadata if it has been defined for the station and nil otherwise"))

(defgeneric spoint-size (station)
  (:documentation "Returns the number of frequencies defined for each of this station's spectral points"))

(defgeneric lat (station)
  (:documentation "Returns the latitude of the station"))

(defgeneric lon (station)
  (:documentation "Returns the longitude of the station"))

(defgeneric depth (station)
  (:documentation "Returns the depth of the station"))

(defgeneric get-spectrum (station time)
  (:documentation "Returns the directional spectrum of the station at the specified time, if it exists"))

(defgeneric station-push (station sp)
  (:documentation "Pushes a spectral point onto end of the station's data"))

(defgeneric begin (station)
  (:documentation "Returns the first time stamp of the station's data"))

(defgeneric clear-data (station)
  (:documentation "Clears the station's data"))

(defmethod initialize-instance :after ((st station) &key (lat 999.9) (lon 999.9) (depth 0.0) (freqs #()))
  (setf (slot-value st 'source) (if (member (id st) *cdip-stations*) 'cdip 'noaa))
  (setf (metadata st) (make-station-metadata :lat lat :lon lon :depth depth :freqs freqs)))

(defmethod freqs ((station station))
  (station-metadata-freqs (metadata station)))

(defmethod (setf freqs) ((freqs simple-vector) (station station))
  (setf (station-metadata-freqs (metadata station)) freqs))

(defmethod freqs-defined ((station station))
  (let ((freqs (freqs station)))
    (when (> (length freqs) 0)
      freqs)))

(defmethod metadata-defined ((station station))
  (unless (or
           (= (length (freqs station)) 0)
           (= (lat station) 999.9)
           (= (lon station) 999.9)
           (= (depth station) 0.0))
    (metadata station)))

(defmethod spoint-size ((station station))
  (length (freqs station)))

(defmethod lat ((station station))
  (station-metadata-lat (metadata station)))

(defmethod lon ((station station))
  (station-metadata-lon (metadata station)))

(defmethod depth ((station station))
  (station-metadata-depth (metadata station)))

(defmethod station-push ((station station) (sp spectral-point))
  (setf (slot-value station 'end) (spectral-point-ts sp))
  (vector-push-extend sp (data station)))

(defmethod begin ((station station))
  (spectral-point-ts (aref (data station) 0)))

(defmethod clear-data ((station station))
  (reset-data-vect (data station)))

