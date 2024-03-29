(defpackage :wavetools/station
  (:use :cl :wavetools/wavespectrum)
  (:nicknames :station)
  (:import-from :lisp-binary
                :defbinary
                :counted-array)
  (:export
   :station
   :id
   :metadata
   :data
   :end
   :source
   :cdip

   :station-metadata
   :make-station-metadata
   :station-metadata-lat
   :station-metadata-lon
   :station-metadata-depth
   :station-metadata-freqs
   
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
   :is-cdip-station

   :dump-station))

(in-package :wavetools/station)

(defvar *cdip-stations* '("46267"))

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
    :type string
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

(defgeneric (setf freqs) (freqs station)
  (:documentation "Sets the frequency array for the station"))

(defgeneric (setf data) (data station)
  (:documentation "Sets the data array for the station"))

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

(defgeneric is-cdip-station (var)
  (:documentation "Returns t if the station described by var is a CDIP station"))

(defmethod is-cdip-station ((id string))
  (member id *cdip-stations* :test 'string=))

(defmethod initialize-instance :after ((st station) &key (data (init-data-vect)))
  (setf (slot-value st 'source) (if (is-cdip-station st) 'cdip 'noaa))
  (setf (data st) data))

(defmethod is-cdip-station ((station station))
  (is-cdip-station (id station)))

(defmethod freqs ((station station))
  (station-metadata-freqs (metadata station)))

(defmethod (setf freqs) ((freqs simple-vector) (station station))
  (setf (station-metadata-freqs (metadata station)) freqs))

(defmethod (setf data) ((data vector) (station station))
  (let ((len (length data)))
    (when (> len 0)
      (setf (slot-value station 'end) (spectral-point-ts (elt data (1- len)))))
    (setf (slot-value station 'data) data)))

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

(defun dump-station (station dir-res &optional stream)
  (let ((binary-out (or
                     stream
                     (sb-ext::make-fd-stream
                      1
                      :name "binary-output"
                      :output t
                      :buffering :none
                      :element-type '(unsigned-byte 8)))))
    (lisp-binary:write-binary-type (id station) '(lisp-binary:counted-string 1) binary-out)
    (lisp-binary:write-binary-type (lat station) 'float binary-out)
    (lisp-binary:write-binary-type (lon station) 'float binary-out)
    (lisp-binary:write-binary-type (freqs station) '(lisp-binary:counted-array 2 float) binary-out)
    (lisp-binary:write-binary-type (create-direction-space dir-res) '(lisp-binary:counted-array 2 float) binary-out)
    (map nil (lambda (spoint)
               (dump-spectrum spoint (freqs station) dir-res binary-out))
         (data station))))
