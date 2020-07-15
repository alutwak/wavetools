
;;(declaim (optimize speed))
;;(declaim (optimize (debug 3)))

(ql:quickload '(:dexador :plump :lquery :lparallel :uiop :str :cl-ppcre :bt-semaphore) :silent t)

;;(load (compile-file "wavespectrum.lisp"))

;;; Utilities

;;; Path definitions and retrieval functions

(defvar *buoy-cache* (uiop:native-namestring "~/.buoy/")
  "Main path for the bouy data cache")

(defun ensure-cache-exists ()
  "Ensures that the cache directory exists."
  (ensure-directories-exist *buoy-cache*))

(defvar *station-cache* (concatenate 'string *buoy-cache* "~D/")
  "Path for caching data for a specific station. The ~D is the station ID.")

(defun get-station-cache-path (station-id)
  (format nil *station-cache* station-id))

(defun ensure-station-cache-exists (station-id)
  "Returns the path of the cache for the given station-id and creates the directory path if it doesn't exist yet"
  (let ((cache-path (get-station-cache-path station-id)))
    (ensure-directories-exist cache-path)))

(defvar *rtd-path* "rtd"
  "Path for real-time data cache. This will contain data for the last 45 days. Some of that may overlap with historical data.
In general, the historical data will be better than the real-time data. This data gets updated at least daily, so this cache
is only really useful when multiple hits on the data are done within a few hours.")

(defun get-rtd-data-path (station-id)
  "Returns the path of the rtd cache for the given station id"
  (concatenate 'string (get-station-cache-path station-id) *rtd-path*))

(defun ensure-rtd-data-path-exists (station-id)
  "Returns the path of the rtd cache for the given station-id and creates the directory path if it doesn't exist yet"
  (let ((cache-path (get-rtd-data-path station-id)))
    (ensure-directories-exist cache-path)
    cache-path))

(defvar *hist-path* "hist/"
  "Path for historical data cache.")

(defun this-year ()
  "Returns the current year"
  (nth-value 5 (get-decoded-time)))

(defun get-year (time)
  "Returns the year of the given universal time"
  (nth-value 5 (decode-universal-time time)))

(defun get-month (time)
  "Returns the month of the given universal time"
  (values (nth-value 4 (decode-universal-time time))
          (local-time:format-timestring nil (local-time:universal-to-timestamp time) :format '(:short-month))))

(defun get-hist-data-path-date (time)
  "Gets the historical data \"time\" path for a given universal time. This path is valid for both the cache and the NOAA urls,
and it is either the year if time is from a previous year, or the month name if time is from this year."
  (multiple-value-bind (s m h d MM YY)
      (decode-universal-time time)
    (if (= YY (this-year))
        (local-time:format-timestring nil (local-time:universal-to-timestamp time) :format '(:short-month))
        YY)))

(defun get-hist-cache-path (station-id &optional month-or-year)
  "Returns the historical data cache path for the given station id and month or year if one is given."
  (format nil "~A~A~A" (get-station-cache-path station-id) *hist-path* month-or-year))

(defun get-hist-cache-path-from-time (station-id time)
  "Returns the historical data cache path for the given station id and universal time"
  (get-hist-cache-path station-id (get-hist-data-path-date time)))

(defun ensure-hist-path-exists (station-id &optional time)
  "Returns the historical data cache path for the given station id and universal time \(if one is given\) and creates the
directory path if it does not exist yet."
  (let ((cache-path (if time
                        (get-hist-cache-path-from-time station-id time)
                        (get-hist-cache-path station-id))))
    (ensure-directories-exist cache-path)))

(defun try-get-url (url)
  "Attempts a request at the given http url and returns the request as a string if it's successful and nil if it gets a 404
error. Other request errors are not handled yet."
  (handler-case
      (dex:get url)
    (dex:http-request-not-found () nil)))

;; ---------------------------------- Station ------------------------------------

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
    :documentation "The last time stamp of the data")))


(defgeneric freqs (station)
  (:documentation "Returns the frequency array for the station"))

(defgeneric (setf freqs) (station freqs)
  (:documentation "Sets the frequency array for the station"))

(defgeneric freqs-defined (station)
  (:documentation "Returns freqs if they have been defined for the station and nil otherwise"))

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

(defmethod freqs ((station station))
  (station-metadata-freqs (metadata station)))

(defmethod (setf freqs) ((freqs simple-vector) (station station))
  (setf (station-metadata-freqs (metadata station)) freqs))

(defmethod freqs-defined ((station station))
  (let ((freqs (freqs station)))
    (when (> (length freqs) 0)
      freqs)))

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

(defun download-station-metadata (station-id)
  (let* ((loc-scan (ppcre:create-scanner "\\s+([0-9.]+) (N|S) ([0-9.]+) (E|W)"))
         (depth-scan (ppcre:create-scanner "\\s+Water depth: ([0-9.]+) m"))
         (request (try-get-url (format nil "https://www.ndbc.noaa.gov/station_page.php?station=~D&uom=E&tz=STN" station-id)))
         (parsed-req (lquery:$ (lquery:initialize request)))
         (metadata-str (aref (lquery:$ parsed-req "#stn_metadata p" (text)) 0)) ; This comes as an annoying paragraph
         (lat)
         (lon)
         (water-depth))
    (dolist (line (str:lines metadata-str) (make-station-metadata :lat lat :lon lon :depth water-depth))
      (let* ((lat-lon? (ppcre:register-groups-bind
                           ((#'read-from-string lat ltdir lon lndir)) 
                           (loc-scan line)
                         (cons (* lat (if (string= ltdir "S") -1 1)) (* lon (if (string= lndir "E") -1 1)))))
             (w-d? (ppcre:register-groups-bind
                       ((#'read-from-string depth))
                       (depth-scan line)
                     (float depth))))
        (when lat-lon?
          (setq lat (car lat-lon?) lon (cdr lat-lon?)))
        (when w-d? (setq water-depth w-d?))))))

;; ----------------------------------- Raw Data (in string format) -------------------------------

(defstruct (raw-data (:constructor make-raw-data (c a1 a2 r1 r2)))
  "Structures a set of raw data for the data parsers. Raw data comes as a set of text files, one for each parameter. Each
parameter is stored in a stream."
  (c nil :type stream :read-only t)
  (a1 nil :type stream :read-only t)
  (a2 nil :type stream :read-only t)
  (r1 nil :type stream :read-only t)
  (r2 nil :type stream :read-only t))

(defun raw-data-from-strings (c-data a1-data a2-data r1-data r2-data)
  "Creates a raw-data object from a set of parameter strings, which is how they will be retrieved from the NOAA server."
  (make-raw-data
   (make-string-input-stream c-data)
   (make-string-input-stream a1-data)
   (make-string-input-stream a2-data)
   (make-string-input-stream r1-data)
   (make-string-input-stream r2-data)))

(defun raw-data-from-paths (c-path a1-path a2-path r1-path r2-path)
  "Creates a raw-data object from a set of parameter file paths, which is how they will be retrieved from the cache."
  (make-raw-data
   (open c-path)
   (open a1-path)
   (open a2-path)
   (open r1-path)
   (open r2-path)))

(defun raw-data->list (rd)
  "Returns the raw-data parameters as a list to make mapping with them easier"
  (list (raw-data-c rd) (raw-data-a1 rd) (raw-data-a2 rd) (raw-data-r1 rd) (raw-data-r2 rd)))

(defun raw-data-close (rd)
  "Closes all of the streams in a raw-data object"
  (mapc #'close (raw-data->list rd)))

(defmacro with-open-rd-files ((rd c-path a1-path a2-path r1-path r2-path) &body body)
  "Opens the given raw data files, creates a raw-data object with them and places that into the rd symbol, then executes body
before closing the files."
  `(handler-case
       (let ((,rd (raw-data-from-paths ,c-path ,a1-path ,a2-path ,r1-path ,r2-path)))
         (prog1
             (progn ,@body)
           (raw-data-close ,rd)))
    (file-does-not-exist () nil)))

(defun raw-data-reset (rd)
  "Resets the streams in a raw-data object to the beginning."
  (mapc (lambda (getter)
          (file-position (funcall getter rd) 0))
        (list #'raw-data-c #'raw-data-a1 #'raw-data-a2 #'raw-data-r1 #'raw-data-r2)))

(defun raw-data-read-line (rd)
  "Reads lines from all raw data streams and returns them"
  (values-list
   (mapcar
    (lambda (strm)
      (read-line strm nil nil))
    (raw-data->list rd))))

(defun eof-p (stream)
  "Returns non-nil if the end of file has been reached on the given stream"
  (not (peek-char nil stream nil nil)))

(defun raw-data-eof-p (rd)
  "Returns non-nil if the end of file has been reached on any of the streams in the given raw-data object."
  (some #'eof-p (raw-data->list rd)))

(defvar *date-scan* (ppcre:create-scanner "(\\d{4}) (\\d{2}) (\\d{2}) (\\d{2}) (\\d{2})")
  "Regex for retrieving the date from a buoy data row")

(defun parse-time (line)
  "Extracts the time from a given raw data line and returns it as a universal timestamp"
  (ppcre:register-groups-bind
      ((#'parse-integer year month day hour min))
      (*date-scan*
       line)
    (encode-universal-time 0 min hour day month year)))

;;; Data Caching

(defun read-data-cache (station path start-time &optional end-time)
  (let ((end-time (if end-time end-time (get-universal-time))))
    (lisp-binary:with-open-binary-file (f path :direction :input)
      (labels ((read-data ()
                 (let* ((sp (lisp-binary:read-binary 'spectral-point f))
                        (time (spectral-point-ts sp)))
                 
                   ;; Read until we reach end-time or the end of the file (at which point, ts will be 0)
                   (cond ((> time end-time)
                          ;; We reached the end time
                          'end)
                         ((= time 0.0)
                          ;; We've reached the end of the file without hitting the end time
                          nil)
                         ((< time start-time)
                          ;; We're not at start-time yet. Keep going, but don't append data
                          (read-data))
                         (t
                          ;; This is data we want to return. Append it and keep going
                          (station-push station sp)
                          (read-data))))))
        (read-data)))))

(defun read-metadata-cache (path)
  (lisp-binary:with-open-binary-file (f path :direction :input)
    (lisp-binary:read-binary 'station-metadata f)))

(defun write-metadata-cache (path &key (if-exists :error))
  (lisp-binary:with-open-binary-file (f path :direction :output :if-exists if-exists)))

;;; Real Time Data

(defun parse-rtd (station rd &optional start-time end-time cache-stream)
  "Parses real-time data formatted strings into the data argument, which must be an adjustable vector. Only data points with
  time stamps between start-time and end-time will be station-pushed into the data vector. If cache-stream is not nil, it must be a
  binary stream and all data points will be written to that stream.

  Each parameter comes in a separate file (which are provided to parse-rtd as strings). Each line of each file starts with a
  5-number date, and the values of the parameter for each frequency are given as: <val1> (<freq1>) <val2> (<freq2>) ...  The
  separation frequency is given in the spec file (which defines the 'c' parameter).

  Returns t if end-time was reached and nil otherwise"
  (let ((fsep-scan (ppcre:create-scanner "([0-9.]+)"))                ; regex for getting the fsep
        (stat-scan (ppcre:create-scanner "([0-9.]+) \\([0-9.]+\\)"))  ; regex for getting the stats
        (freq-scan (ppcre:create-scanner "[0-9.]+ \\(([0-9.]+)\\)"))) ; regex for getting the freqs
    (labels ((parse-line (line c-data?)
               "Parses a single line from one of the parameter files.

                If c-data? is non-nil, the fsep data and frequencies are captured"
               (let ((fsep (if c-data?
                               (ppcre:register-groups-bind ((#'read-from-string fsep))
                                   (fsep-scan
                                    line
                                    :start 17)
                                 fsep)
                               nil))
                     (data (make-array (spoint-size station) :element-type 'float :initial-element 0.0))
                     (dref -1))
                 (ppcre:do-register-groups ((#'read-from-string stat))
                     (stat-scan
                      line
                      nil
                      :start (if c-data? 23 17))
                   (progn
                     (setf (aref data (incf dref)) (float stat))))
                 (list data fsep)))

             (parse-all-lines (c-line a1-line a2-line r1-line r2-line time)
               "Parses lines for all raw data streams"
               (let* ((c-fsep (parse-line c-line t))
                      (a1 (car (parse-line a1-line nil)))
                      (a2 (car (parse-line a2-line nil)))
                      (r1 (car (parse-line r1-line nil)))
                      (r2 (car (parse-line r2-line nil))))
                 (make-spect-point time (car c-fsep) a1 a2 r1 r2 (cadr c-fsep))))

             (parse-next-entry (parsed)
               "Recursively reads lines from the raw data streams and parses them to create spectral points"
               (multiple-value-bind
                     (c a1 a2 r1 r2)
                   (raw-data-read-line rd)
                 (let ((time (parse-time c)))
                   (if (raw-data-eof-p rd)
                       parsed
                       (parse-next-entry (cons (parse-all-lines c a1 a2 r1 r2 time) parsed)))))))

      ;; Get the frequencies if we don't have them yet
      (when (not (freqs-defined station))
          (raw-data-read-line rd) ;; Discard header      
          (let ((freqs))
            (ppcre:do-register-groups ((#'read-from-string freq))
                (freq-scan
                 (raw-data-read-line rd)
                 nil
                 :start 23)
              (setq freqs (cons freq freqs)))
            (setf (freqs station) (apply #'vector (reverse freqs))))

          (raw-data-reset rd))     ;; Reset the data

      ;; Now, parse the data
      (raw-data-read-line rd) ;; Discard header

      ;; RT data comes in reverse time order, so we need to cons each line and then map back over to put it in order
      (let ((parsed (parse-next-entry '())))
        (reduce (lambda (reached-end sp)
                  ;; Write everything to the cache when it's there
                  (when cache-stream (lisp-binary:write-binary sp cache-stream))

                  ;; Only return data that's between the start and end times when they're given
                  (let ((time (spectral-point-ts sp)))
                    (cond ((and start-time (< time start-time))
                           nil)
                          ((and end-time (> time end-time))
                           'end)
                          (t
                           (station-push station sp)
                           nil))))
                parsed)))))

(defun download-station-rtd (station &optional start-time end-time cache-data)
  "Downloads the real-time data for the specified station id, and appends it to the data argument, which must be an
  adjustable vector. If start-time is non-nil, then only data points after that \(universal\) time will be appended. If
  end-time is non-nil then only data points before that time will be appended. If cache-data is non-nil, the all downloaded
  data will saved in the data cache.

  Returns non-nil value if end-time was reached and nil otherwise."
  (flet ((get-data (path)
           (try-get-url (format nil "https://www.ndbc.noaa.gov/data/realtime2/~D.~A" (id station) path))))
    (let ((raw-data `(,(get-data "data_spec")
                       ,(get-data "swdir")
                       ,(get-data "swdir2")
                       ,(get-data "swr1")
                       ,(get-data "swr2"))))

      ;; Parse using cache file if we're caching, and without it if we're not
      (if cache-data 
          (lisp-binary:with-open-binary-file (f (ensure-rtd-data-path-exists (id station)) :direction :output)
            (parse-rtd station (apply #'raw-data-from-strings raw-data) start-time end-time f))
          (parse-rtd station (apply #'raw-data-from-strings raw-data) start-time end-time)))))

(defun read-station-rtd-cache (station &optional (start-time 0) end-time)
  "Reads real-time data from the rtd cache for the given station id. If start-time is non-nil, then only data points after
that \(universal\) time will be returned. If end-time is non-nil then only data points before that time will be returned. 

If the rtd cache for the given station does not exist, then the return value will be nil."
  (let ((cache-path (get-rtd-data-path (id station))))
    (read-data-cache station cache-path start-time end-time)))


;;; Historical Data

;; regex for getting the stat and freq
(defvar *hist-stat-freq-scan* (ppcre:create-scanner "\\s+([0-9.]+)"))

(defun parse-hist (station rd &optional start-time end-time cache-stream)
  "Parses historical data formatted strings into the data argument, which must be an adjustable vector. Only data points with
  time stamps between start-time and end-time will be station-pushed into the data vector. If cache-stream is not nil, it must be a
  binary stream and all data points will be written to that stream.

  Each parameter comes in a separate stream. Each line of each stream starts with a 5-number date, followed by a series of
  space-separated floating point values. The header for each file contains the set of frequencies, one for each row of data.

  Returns t if end-time was reached and nil otherwise."
  (labels ((parse-line (line)
             "Parses a single line from one of the parameter files."
             (let ((data (make-array (spoint-size station) :element-type 'float :initial-element 0.0))
                   (dref -1))
               (ppcre:do-register-groups ((#'read-from-string stat))
                   (*hist-stat-freq-scan*
                    line
                    nil
                    :start 16)
                 (setf (aref data (incf dref)) (float stat)))
               data))
           
           (parse-all-lines (c-line a1-line a2-line r1-line r2-line time)
             "Parses lines for all raw data streams"
             (let* ((c (parse-line c-line))
                    (a1 (parse-line a1-line))
                    (a2 (parse-line a2-line))
                    (r1 (parse-line r1-line))
                    (r2 (parse-line r2-line)))
               (make-spect-point time c a1 a2 r1 r2)))

           (parse-next-entry (reached-end)
             "Recursively reads lines from the raw data streams and parses them to create spectral points"
             (multiple-value-bind
                   (c a1 a2 r1 r2)
                 (raw-data-read-line rd)
               (let* ((time (parse-time c))
                      (sp (parse-all-lines c a1 a2 r1 r2 time)))
                 (when cache-stream (lisp-binary:write-binary sp cache-stream))
                 (unless (or (and start-time (< time start-time)) (and end-time (> time end-time)))
                   (station-push station sp))
                 (cond ((raw-data-eof-p rd) ;; We're done parsing so return 
                        reached-end)
                       (t ;; Keep going
                        (parse-next-entry (> time end-time))))))))

    ;; Get the frequencies
    (when (not (freqs-defined station))
      (let ((freqs))
        (ppcre:do-register-groups ((#'read-from-string freq))
            (*hist-stat-freq-scan*
             (raw-data-read-line rd)    ; Returns first line of c data and throws away the other data files' lines
             nil
             :start 16)
          (setq freqs (cons freq freqs)))
        
        ;; Reverse them to put them in order      
        (setf (freqs station) (apply #'vector (reverse freqs)))))

    ;; Return parsed data
    (parse-next-entry nil)))

;; There are two (so far) url formats for monthly data - annoying, I know. Most of them seem to be the first format, but
;; (at least at the time of this writing) the latest month comes in the second format. The easiest thing to do is to just try
;; the first url and then try the second if the first doesn't work.
(defvar *hist-month-urls*
  (list
   (lambda (station-id path time)
     (multiple-value-bind (m-num m-str) (get-month time)
       (format
        nil
        "https://www.ndbc.noaa.gov/view_text_file.php?filename=~D~D~D.txt.gz&dir=data/~A/~A/"
        station-id
        m-num
        (get-year time)
        path
        m-str)))
   (lambda (station-id path time)
     (format
      nil
      "https://www.ndbc.noaa.gov/data/~A/~A/~D.txt"
      path
      (nth-value 1 (get-month time))
      station-id))))

(defun download-station-hist (station start-time &optional end-time cache-data)
  "Downloads the historical data \(if it exists\) in which the given start time is stored for the given station id. Only data
  points with times equal to or later than the start time will be returned and, if non-nil, only data points with times
  before end-time will be returned.

  Historical data is kept in yearly chunks for each year up to the current one. For the current year, data is kept in monthly
  chunks. The url format for each is slightly different, so we need to choose the url based on whether time is from this year
  \(at the time that this function is called\) or from a previous year.

  download-station-hist will download all data for the year/month of the start-time parameter, and it will parse the data
  until the end-time or the end of the data is reached.

  If cache-data is non-nil, the downloaded data will be cached locally for future use."
  (labels ((get-year-data (char-key path)
             "Gets data at a 'year' url"
             (try-get-url
              (format
               nil
               "https://www.ndbc.noaa.gov/view_text_file.php?filename=~D~C~D.txt.gz&dir=data/historical/~A/"
               (id station)
               char-key
               (get-year start-time)
               path)))
           (get-month-data (path)
             (or (try-get-url (funcall (car *hist-month-urls*) (id station) path start-time))
                 (try-get-url (funcall (cadr *hist-month-urls*) (id station) path start-time))))
           (get-data (char-key path)
             (if (= (get-year start-time) (this-year))
                 (get-month-data path)
                 (get-year-data char-key path))))
    (let ((raw-data `(,(get-data #\w "swden")
                       ,(get-data #\d "swdir")
                       ,(get-data #\i "swdir2")
                       ,(get-data #\j "swr1")
                       ,(get-data #\k "swr2"))))

      ;; Parse using cache file if we're caching, and without it if we're not
      (if cache-data 
          (lisp-binary:with-open-binary-file (f (ensure-hist-path-exists (id station) start-time) :direction :output)
            (parse-hist station (apply #'raw-data-from-strings raw-data) start-time end-time f))
          (parse-hist station (apply #'raw-data-from-strings raw-data) start-time end-time)))))

(defun read-station-hist-cache (station start-time &optional end-time)
  "Reads the historical data from the cache \(if it exists\) in which the given start time is stored for the given station
  id. Only data points with times equal to or later than the start time will be returned and, if non-nil, only data points
  with times before end-time will be returned."
  (let ((cache-path (get-hist-cache-path-from-time (id station) start-time)))
    (read-data-cache station cache-path start-time end-time)))

(defun get-hist-data (station start-time end-time &optional cache-new-data)
  "Returns all available historical data, preferably from the cache, if it's available there, between start-time and end-time
for the given station id. If cache-new-data is non-nil, then any data not found in the cache, and retrieved from the web will
be cached for later use."
  (labels ((get-data (start)
             (format t
                     "start: ~A~%"
                     (local-time:format-timestring nil (local-time:universal-to-timestamp start)
                                                   :format local-time:+asctime-format+))
             (or (read-station-hist-cache station start end-time)
                 (download-station-hist station start end-time cache-new-data))))
    (do* ((end-reached (get-data start-time) (get-data next-start))
          (next-start))
         (end-reached station) ; We're done when get-data returns non-nil
      
      ;; Make sure that the next start time is at the beginning of the next hist file
      ;; This will be the beginning of the next month in all cases
      (let* ((last-end (end station)))
        (format
         t
         "last-end: ~A~%"
         (local-time:format-timestring nil (local-time:universal-to-timestamp last-end)
                                       :format local-time:+asctime-format+))
        (setq next-start
              (multiple-value-bind (s m h d mon yr) (decode-universal-time last-end)
                (let* ((mon (1+ mon))
                       (yr (if (> mon 12) (1+ yr) yr)))
                  (encode-universal-time 0 0 0 1 (1+ (mod (1- mon) 12)) yr))))))))

;; General Data Retrieval API

