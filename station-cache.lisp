(defpackage :wavetools/station-cache
  (:use :cl :wavetools/station :wavetools/wavespectrum)
  (:import-from :mito
                :deftable)
  (:export :write-station
           :with-cache-writer
           :read-cache))


(in-package :wavetools/station-cache)


(defvar *station-cache* (uiop:native-namestring "~/wavestation/")
  "Base path for the station data cache")

(defvar *hist-cache* (concatenate 'string "hist.sqlite")
  "Path for the station historical data cache")

(defvar *rtd-cache* (concatenate 'string "rtd.sqlite")
  "Path for the station real-time data cache")

(defun ensure-station-cache-exists ()
  "Returns the path of the cache for the given station-id and creates the directory path if it doesn't exist yet"
  (ensure-directories-exist *station-cache*))

;;; ======================= Utility functions for converting between lisp types and database types ====================

(defun float-to-blob (flt)
  "Converts a float array to a binary blob, which is encoded as a uint8 array"
  (let* ((flen (length flt))
         (blen (* 4 flen)))
    (make-array `(,blen) :element-type '(unsigned-byte 8) :initial-contents
                (cffi:with-foreign-array (blob flt `(:array :float ,flen))
                  (loop for i below flen
                        append 
                        (loop for j below 4
                              collect (cffi:mem-aref blob :uint8 (+ j (* 4 i)))))))))

(defun blob-to-float (blob)
  "Converts a binary blob, encoded as a uint8 array, to a float array. 
   
  The length of blob is not checked to ensure that it is the correct size for a float array. Since this function is just
  being used internally to convert blobs that have already been converted from float arrays, it seems like a waste of time to
  do that check, but it might be worth it if I find that data is getting corrupted somewhere, or if this function gets used
  in another context."
  (let* ((blen (length blob))
         (flen (/ blen 4)))
    (make-array `(,flen) :element-type 'float :initial-contents
                (cffi:with-foreign-array (array blob `(:array :uint8 ,blen))
                  (loop for i below flen
                        collect (cffi:mem-aref array :float i))))))

(defmacro with-db-connection ((to-rtd) &body body)
  (let ((connection (gensym)))
   `(let ((,connection (dbi:connect :sqlite3 :database-name (if ,to-rtd *rtd-cache* *hist-cache*))))
      (unwind-protect
           (progn
             ,@body)
        ;; Ensure that the connection is closed.
        (dbi:disconnect ,connection)))))

;;; ======================================== Generic database functions ======================================

(defgeneric to-table (obj &key)
  (:documentation "Creates a table object from a normal object"))

(defgeneric from-table (row)
  (:documentation "Creates a normal object from a table object"))

(defun safe-insert (row)
  (handler-case
      (mito:save-dao row)
    (dbi.error:dbi-database-error (condition)
      (with-slots ((message dbi.error::message) (code dbi.error::error-code)) condition
        (cond ((and (eq code :constraint) (string= "UNIQUE" (subseq message 0 6))) nil) ;; (format t "~A~%" message)
              (t (format t "Unhandled database error: ~A -- ~A~%" code (type-of code))
                 (error condition)))))))

;;; ======================================= Station table ===================================================

(defclass station-table ()
  ((id :col-type :integer :primary-key t
       :accessor id)
   (lat :col-type :real
        :accessor lat)
   (lon :col-type :real
        :accessor lon)
   (depth :col-type :real
          :accessor depth)
   (freqs :col-type (or :integer :null)
          :accessor freqs)
   (start-time :col-type (or :integer :null)
               :accessor start-time)
   (end-time :col-type (or :integer :null)
             :accessor end-time)
   (source :col-type :text
           :accessor source))
  (:metaclass mito:dao-table-class))

(defmethod to-table ((station station) &key)
  (make-instance
   'station-table :id (id station) :lat (lat station) :lon (lon station) :depth (depth station)
                  :freqs (float-to-blob (freqs station)) :source (source station)))

(defmethod from-table ((st station-table))
  (make-instance
     'station :id (id st) :lat (lat st) :lon (lon st) :depth (depth st)
     :freqs (blob-to-float (freqs st))))

(defun insert-station (station)
  "Inserts a station object into a station-table row. TODO: Update the start-time and end-time values"
  (let ((row (to-table station)))
    (safe-insert row)))

(defun extract-station (station-id)
  (let ((row (car (mito:retrieve-dao 'station-table :id station-id))))
    (from-table row)))

;;; ======================================== Spectral Point table ============================================


(defclass spectral-point-table ()
  ((station-id :col-type :integer
    :accessor station-id)
   (ts :col-type :integer
       :accessor ts)
   (id :col-type :integer :primary-key t
           :reader id)
   (c :col-type :blob
      :accessor c)
   (a1 :col-type :blob
       :accessor a1)
   (a2 :col-type :blob
       :accessor a2)
   (r1 :col-type :blob
       :accessor r1)
   (r2 :col-type :blob
       :accessor r2)
   (fsep :col-type (or :real :null)
         :accessor fsep)
   (hs :col-type (or :real :null)
       :accessor hs)
   (tp :col-type (or :real :null)
       :accessor tp)
   (dp :col-type (or :real :null)
       :accessor dp))
  (:metaclass mito:dao-table-class))

(defmethod initialize-instance :after ((spt spectral-point-table) &key)
  (setf (slot-value spt 'id) (logior (ash (station-id spt) 33) (ts spt))))

(defmethod to-table ((sp spectral-point) &key (station-id 0))
  (make-instance
   'spectral-point-table :station-id station-id :ts (spectral-point-ts sp)
                         :c (float-to-blob (spectral-point-c sp))
                         :a1 (float-to-blob (spectral-point-a1 sp)) :a2 (float-to-blob (spectral-point-a2 sp))
                         :r1 (float-to-blob (spectral-point-r1 sp)) :r2 (float-to-blob (spectral-point-r2 sp))
                         :fsep (spectral-point-fsep sp)))

(defmethod from-table ((spt spectral-point-table))
  (make-spect-point
   (ts spt)
   (blob-to-float (c spt))
   (blob-to-float (a1 spt))
   (blob-to-float (a2 spt))
   (blob-to-float (r1 spt))
   (blob-to-float (r2 spt))
   (fsep spt)))

(defun insert-spectral-point (sp station-id)
  (let ((row (to-table sp :station-id station-id)))
    (safe-insert row)))

(defun insert-data (station)
  (let ((id (id station)))
    (map nil (lambda (sp)
               (insert-spectral-point sp id))
         (data station))))

(defun extract-data (station start-time end-time)
  (let ((data
          (mito:select-dao 'wavetools/station-cache::spectral-point-table
            (sxql:where `(:and (:>= :ts ,start-time) (:<= :ts ,end-time) (:= :station-id ,(id station))))
            (sxql:order-by :ts))))
    (setf (data station) (map 'vector #'from-table data))
    station))

;;; ================================= Cache API =============================================================

(defun write-station (station &optional to-rtd)
  "Writes the station info to the cache. Uses the rtd cache if to-rtd is non-nil"
  (ensure-station-cache-exists)
  (with-db-connection (to-rtd)
    (mito:ensure-table-exists 'station-table)
    (insert-station station)))

(defmacro with-cache-writer ((cache-writer station-id &optional to-rtd) &body body)
  "Creates a function for writing spectral points to the cache for the station with the given id and executes the body. Uses
   the rtd cache if to-rtd is non-nil."
  (ensure-station-cache-exists)
  `(with-db-connection (,to-rtd)
     (flet ((,cache-writer (sp)
              (insert-spectral-point sp ,station-id)))
       ,@body)))

(defun read-cache (station-id start-time end-time &optional from-rtd)
  "Reads all cached data between start-time and end-time into the given station. If from-rtd is non-nil, the data is read
  from the real-time data cache"
  (with-db-connection (from-rtd)
    (let ((station (extract-station station-id)))
      (extract-data station start-time end-time))))

