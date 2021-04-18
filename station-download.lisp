; (defpackage :wavetools/station-download
;;   (:use :cl :sb-ext :wavetools/wavespectrum :wavetools/station :wavetools/station-cache)
;;   (:import-from :lisp-binary
;;                 :write-binary
;;                 :with-open-binary-file)
;;   (:import-from :cl-ppcre
;;                 :do-register-groups
;;                 :create-scanner
;;                 :register-groups-bind)
;;   (:import-from :dexador
;;                 :http-request-not-found)
;;   (:import-from :lquery
;;                 :$
;;                 :initialize)
;;   (:export :download-station-data
;;            :download-station-metadata
;;            :download-station-rtd))

(in-package :wavetools)

;;; ------------------------ Utility Functions --------------------------------------------

(defun this-year ()
  "Returns the current year"
  (nth-value 5 (get-decoded-time)))

(defun get-year (time)
  "Returns the year of the given universal time"
  (nth-value 5 (decode-universal-time time 0)))

(defun get-month (time)
  "Returns the month of the given universal time"
  (values (nth-value 4 (decode-universal-time time 0))
          (local-time:format-timestring nil (local-time:universal-to-timestamp time) :format '(:short-month))))

(defun try-get-url (url)
  "Attempts a request at the given http url and returns the request as a string if it's successful and nil if it gets a 404
error. Other request errors are not handled yet."
  (handler-case
      (dex:get url)
    (dex:http-request-not-found () nil)))

(defvar *pi-single* (coerce pi 'single-float))

(defun deg-to-rad (deg)
  (/ (* *pi-single* deg) 180.0))

;; ========================================= Raw Data Retrieval and Handling ===========================================

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
         (unwind-protect
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
    (encode-universal-time 0 min hour day month year 0)))


;;; ------------------------------------------ Metadata ==================================================================

(defun download-noaa-station-metadata (station-id)
  "Downloads and returns the metadata for the given station id"
  (handler-case
      (let* ((loc-scan (ppcre:create-scanner "\\s+([0-9.]+) (N|S) ([0-9.]+) (E|W)"))
             (depth-scan (ppcre:create-scanner "\\s+Water depth: ([0-9.]+) m"))
             (request (dex:get (format nil "https://www.ndbc.noaa.gov/station_page.php?station=~A&uom=E&tz=STN" station-id)))
             (parsed-req (lquery:$ (lquery:initialize request)))
             (metadata-str (aref (lquery:$ parsed-req "#stn_metadata p" (text)) 0)) ; This comes as an annoying paragraph
             (lat)
             (lon)
             (water-depth))
        (dolist (line (str:lines metadata-str) (make-station-metadata :lat lat :lon lon :depth water-depth))
          (let* ((lat-lon? (ppcre:register-groups-bind
                               ((#'read-from-string lat ltdir lon lndir)) 
                               (loc-scan line)
                             (cons (* lat (if (string= ltdir "S") -1 1)) (* lon (if (string= lndir "W") -1 1)))))
                 (w-d? (ppcre:register-groups-bind
                           ((#'read-from-string depth))
                           (depth-scan line)
                         (float depth))))
            (when lat-lon?
              (setq lat (car lat-lon?) lon (cdr lat-lon?)))
            (when w-d? (setq water-depth w-d?)))))
    (dex:http-request-not-found () nil)))

(defun download-cdip-station-metadata (station-id)
  "Downloads the metadata for a cdip station. This function is very inefficient because it downloads the data for the entire
   history of the station just to get the metadata. download-station-cdip will automatically download the metadata, so you
   don't need to call this function unless you just want to get the metadata and no other data."
  (let* ((args `("cdipbuoy.py" ,station-id "-s" "1" "-e" "0" "-m"))
         (proc-var (sb-ext:run-program "python" args :search t :output :stream :wait nil))
         (output (sb-ext:process-output proc-var)))
    (lisp-binary:read-binary 'station-metadata output)))

;;; ========================================= Historical Data ==========================================

;; regex for getting the stat and freq
(defvar *hist-stat-freq-scan* (ppcre:create-scanner "\\s+([0-9.]+)"))

(defun parse-hist (station-id data rd &optional start-time end-time cache-writer)
  "Parses historical data formatted strings into the data argument, which must be an adjustable vector. Only data points with
  time stamps between start-time and end-time will be station-pushed into the data vector. If cache-stream is not nil, it
  must be a binary stream and all data points will be written to that stream.

  Each parameter comes in a separate stream. Each line of each stream starts with a 5-number date, followed by a series of
  space-separated floating point values. The header for each file contains the set of frequencies, one for each row of data.

  Parameters
  ----------
  station-id : string
      the station ID
  data : vector
      An adjustable vector to which data will be appended
  rd : raw-data
      The raw data streams to read from 
  start-time : integer
      The start time as a universal time
  end-time : integer
      The end time as a universal time
  cache-writer : function (spectral-point)
      Will write spectral points to a cache

  Returns
  -------
  status data-array freq-array, where status either 'eot or 'eof
"
  (labels ((parse-line (line sp-size &optional (mult 1.0))
             "Parses a single line from one of the parameter files."
             (let ((data))
               (ppcre:do-register-groups ((#'read-from-string stat))
                   (*hist-stat-freq-scan* line nil :start 16)
                 (push stat data))
               (apply #'vector (nreverse data))))
           
           (parse-all-lines (c-line a1-line a2-line r1-line r2-line time sp-size)
             "Parses lines for all raw data streams"
             (let* ((c (parse-line c-line sp-size))
                    (a1 (map 'vector #'deg-to-rad (parse-line a1-line sp-size)))
                    (a2 (map 'vector #'deg-to-rad (parse-line a2-line sp-size)))
                    (r1 (parse-line r1-line sp-size 0.01))      ;; r1 and r2 values are scaled by 100 in historical data
                    (r2 (parse-line r2-line sp-size 0.01)))     ;; We need to unscale them
               (make-spect-point time c a1 a2 r1 r2)))

           (parse-next-entry (data freqs reached-end)
             "Recursively reads lines from the raw data streams and parses them to create spectral points"
             (multiple-value-bind
                   (c a1 a2 r1 r2)
                 (raw-data-read-line rd)
               (let* ((time (parse-time c))
                      (sp (parse-all-lines c a1 a2 r1 r2 time (length freqs)))
                      (end-status (or reached-end (when (and end-time (> time end-time)) 'eot))))
                 (when cache-writer (funcall cache-writer sp))
                 (unless (or (and start-time (< time start-time)) end-status)
                   (vector-push-extend sp data))
                 (cond ((raw-data-eof-p rd) ;; We're done parsing so return
                        (values
                         (or reached-end 'eof) ;; reached-end if it's 'eot, otherwise return 'eof
                         data
                         freqs)) 
                       (t ;; Keep going
                        (parse-next-entry data freqs end-status)))))))

    (format t "Parsing data~%")
    (let ((freqs (map
                  'vector
                  #'read-from-string
                  (ppcre:all-matches-as-strings
                   *hist-stat-freq-scan*
                   (raw-data-read-line rd)  ; Returns first line of c data and throws away the other data files' lines
                   :start 16))))
      (parse-next-entry data freqs nil))))

;; There are two (so far) url formats for monthly data - annoying, I know. Most of them seem to be the first format, but
;; (at least at the time of this writing) the latest month comes in the second format. The easiest thing to do is to just try
;; the first url and then try the second if the first doesn't work.
(defvar *hist-month-urls*
  (list
   (lambda (station-id path time)
     (multiple-value-bind (m-num m-str) (get-month time)
       (format
        nil
        "https://www.ndbc.noaa.gov/view_text_file.php?filename=~A~D~D.txt.gz&dir=data/~A/~A/"
        station-id
        m-num
        (get-year time)
        path
        m-str)))
   (lambda (station-id path time)
     (format
      nil
      "https://www.ndbc.noaa.gov/data/~A/~A/~A.txt"
      path
      (nth-value 1 (get-month time))
      station-id))))

(defun download-station-hist-next-chunk (station-id data start-time &optional end-time cache-writer)
  "Downloads the historical data (if it exists) in which the given start time is stored for the given station id. Only data
  points with times equal to or later than the start time will be returned and, if non-nil, only data points with times
  before end-time will be returned.

  Historical data is kept in yearly chunks for each year up to the current one. For the current year, data is kept in monthly
  chunks. The url format for each is slightly different, so we need to choose the url based on whether time is from this year
  (at the time that this function is called) or from a previous year.

  download-station-hist will download all data for the year/month of the start-time parameter, and it will parse the data
  until the end-time or the end of the data is reached.

  If cache-data is non-nil, the downloaded data will be cached locally for future use.

  Parameters
  ----------
  station-id : string
      the station ID
  data : vector
      An adjustable vector to which data will be appended
  start-time : integer
      The start time as a universal time
  end-time : integer
      The end time as a universal time
  cache-writer : function (spectral-point)
      Will write spectral points to a cache

  Returns
  -------
  status data-array freq-array, where status either 'eot or 'eof
"
  (labels ((get-year-data (char-key path)
             "Gets data at a 'year' url"
             (dex:get
              (format
               nil
               "https://www.ndbc.noaa.gov/view_text_file.php?filename=~D~C~D.txt.gz&dir=data/historical/~A/"
               station-id
               char-key
               (get-year start-time)
               path)))
           (get-month-data (path)
             (or (try-get-url (funcall (car *hist-month-urls*) station-id path start-time))
                 (dex:get (funcall (cadr *hist-month-urls*) station-id path start-time))))
           (is-month-date (date)
             (= (get-year date) (this-year)))
           (get-data (char-key path)
             (if (is-month-date start-time)
                 (get-month-data path)
                 (get-year-data char-key path))))

    ;; Get the raw data and parse it
    (handler-case
        (let ((raw-data (mapcar #'get-data '(#\w #\d #\i #\j #\k) '("swden" "swdir" "swdir2" "swr1" "swr2"))))
          (parse-hist station-id data (apply #'raw-data-from-strings raw-data) start-time end-time cache-writer))

      ;; If the data don't exist for this time, return nil
      (dex:http-request-not-found ()
        (progn
          (format t "Historical data not found for this date~%")
          ;; Return nil (nothing left to search) if:
          ;;    end-time is nil & (= (get-month end-time) (get-month now))
          ;;    (is-month-date start-time) & (= (get-month start-date) (get-month end-date))
          ;;    (is-year-date start-time) & (= (get-year start-date) (get-year end-date))
          (if (or (and (not end-time)
                       (= (get-month end-time) (get-month (get-universal-time))))
                  (and (is-month-date start-time)
                       (= (get-month start-time) (get-month end-time)))
                  (and (not (is-month-date start-time))
                       (= (get-year start-time) (get-year end-time))))
              (progn (format t "No dates left to search~%") nil)
              'eof))))))

(defun download-station-hist (station-id start-time end-time &optional cache-writer)
  " Downloads the historical data for the given station between start-time and end-time

  If cache-data is non-nil, the downloaded data will be cached locally for future use.

  Parameters
  ----------
  station-id : string
      the station ID
  start-time : integer
      The start time as a universal time
  end-time : integer
      The end time as a universal time
  cache-writer : function (spectral-point)
      Will write spectral points to a cache

  Returns
  -------
  (data-array . station-metadata), or nil if no data were found
"
  (labels ((calculate-next-start (last-start)
             (multiple-value-bind (s m h d mon yr) (decode-universal-time last-start 0)
               (declare (ignore s m h d))
               (let* ((mon (if (= yr (this-year))
                               (1+ (mod mon 12)) ;; This year, we only increment by months
                               1))               ;; Previous years, we increment by years
                      (yr (if (= mon 1)
                              (1+ yr)
                              yr)))
                 (encode-universal-time 0 0 0 1 mon yr 0))))
           (download-all (data next-start)
             (multiple-value-bind (s m h d mon yr) (decode-universal-time next-start 0)
               (declare (ignore s))
               (format t "Downloading data for time: ~D-~D-~D ~D:~D~%"
                       mon d yr h m))
             (multiple-value-bind
                   (end-reached data freqs)
                 (download-station-hist-next-chunk station-id data next-start end-time cache-writer)
               (cond ((equal end-reached 'eot)
                      ;; We've reached end-time
                      (format t "All requested data found ~%")
                      (values end-reached data freqs))
                     ((and (not end-reached))
                      ;; Not all data were found and there's no newer data to search
                      (format t "All requested data not found ~%")
                      ;; return 'eof if there is some data, and nil if there is no data
                      (and (> (length data) 0) (values 'eof data freqs)))
                     (t
                      ;; Either data were found, or they weren't but there may be later data. Either way, keep searching
                      (download-all data (calculate-next-start next-start)))))))
    (format t "Searching for historical data for station ~D~%" station-id)
    (multiple-value-bind (end-reached data freqs)
        (download-all (init-data-vect) start-time)
      (when end-reached
        (let ((metadata (download-noaa-station-metadata station-id)))
          (setf (station-metadata-freqs metadata) freqs)
          (cons data metadata))))))

;;; ======================================== Real Time Data ========================================================

(defun parse-rtd (station-id rd &optional start-time end-time cache-writer)
  "Parses real-time data formatted strings into the data argument, which must be an adjustable vector. Only data points with
  time stamps between start-time and end-time will be station-pushed into the data vector. If cache-stream is not nil, it must be a
  binary stream and all data points will be written to that stream.

  Each parameter comes in a separate file (which are provided to parse-rtd as strings). Each line of each file starts with a
  5-number date, and the values of the parameter for each frequency are given as: <val1> (<freq1>) <val2> (<freq2>) ...  The
  separation frequency is given in the spec file (which defines the 'c' parameter).

  Returns 'eot if end-time was reached and 'eof otherwise"
  (let ((freqs #())
        (data (init-data-vect))
        (fsep-scan (ppcre:create-scanner "([0-9.]+)"))                ; regex for getting the fsep
        (stat-scan (ppcre:create-scanner "([0-9.]+) \\([0-9.]+\\)"))  ; regex for getting the stats
        (freq-scan (ppcre:create-scanner "\\(([0-9.]+)\\)"))) ; regex for getting the freqs
    (labels ((parse-line (line c-data?)
               "Parses a single line from one of the parameter files.

                If c-data? is non-nil, the fsep data and frequencies are captured"
               (let ((fsep (when c-data?
                               (ppcre:register-groups-bind (fsep)
                                   (fsep-scan line :start 17)
                                 (read-from-string fsep))))
                     (data))
                 (ppcre:do-register-groups ((#'read-from-string stat))
                     (stat-scan line nil :start (if c-data? 23 17))
                   (push stat data))
                 (list (apply #'vector (nreverse data)) fsep)))

             (parse-all-lines (c-line a1-line a2-line r1-line r2-line time)
               "Parses lines for all raw data streams"
               (let* ((c-fsep (parse-line c-line t))
                      (a1 (map 'vector #'deg-to-rad (car (parse-line a1-line nil))))
                      (a2 (map 'vector #'deg-to-rad (car (parse-line a2-line nil))))
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

      ;; Frequencies are interspersed with the data values, so read the first data line to get the frequencies,
      ;; and then reset the rd files and start over, reading the data the second time through
      (raw-data-read-line rd) ;; Discard header      

      ;; Get the frequencies
      
      (setq freqs (apply #'vector
                       (let ((freqs*))
                         (ppcre:do-register-groups ((#'read-from-string freq)) 
                             ("\\(([0-9.]+)\\)"
                              (raw-data-read-line rd)) ; Returns first line of c data
                           (push freq freqs*))
                         (nreverse freqs*))))

      (raw-data-reset rd) ;; Reset the data

      ;; Now, parse the data
      (raw-data-read-line rd) ;; Discard header

      (let* ((parsed (parse-next-entry '()))  ;; Parse the data

             ;; Then select the data between start and end
             (selected              
               (remove-if-not
                (lambda (sp)
                  
                  ;; Write everything to the cache when it's there
                  (when cache-writer (funcall cache-writer sp))

                  ;; Only return data that's between the start and end times when they're given
                  (let ((time (spectral-point-ts sp)))
                    (and
                     (or (not start-time) (>= time start-time))
                     (or (not end-time) (<= time end-time)))))
                parsed)))
        (when selected
          (values (apply #'vector selected) freqs))))))

(defun download-station-rtd (station-id &optional start-time end-time cache-writer)
  "Downloads the real-time data for the specified station. If start-time is non-nil, then only data points after that
  \(universal\) time will be appended. If end-time is non-nil then only data points before that time will be appended. If
  cache-writer is non-nil, it must be a cache-writing function.

  Parameters
  ----------
  station-id : string
      the station ID
  start-time : integer
      The start time as a universal time
  end-time : integer
      The end time as a universal time
  cache-writer : function (spectral-point)
      Will write spectral points to a cache

  Returns
  -------
  (data-array . station-metadata), or nil if no data were found

"
  (flet ((get-data (path)
           (dex:get (format nil "https://www.ndbc.noaa.gov/data/realtime2/~A.~A" station-id path))))
    (format t "Downlading real-time-data for station ~A~%" station-id)
    (handler-case
        (let* ((raw-data (mapcar #'get-data '("data_spec" "swdir" "swdir2" "swr1" "swr2"))))

          ;; Parse using cache file if we're caching, and without it if we're not
          (multiple-value-bind (data freqs)
              (parse-rtd station-id (apply #'raw-data-from-strings raw-data) start-time end-time cache-writer)
            (when data
              (let ((metadata (download-noaa-station-metadata station-id)))
                (setf (station-metadata-freqs metadata) freqs)
                (cons data metadata)))))
      (dex:http-request-not-found () nil))))

;;; ========================================== CDIP Stations ========================================================

(defvar *python-time-offset* (encode-universal-time 0 0 0 1 1 1970 0))

(defun download-station-cdip (station-id start-time end-time &optional cache-writer)
  (let* ((args `("cdipbuoy.py"
                 ,station-id
                 "-s" ,(write-to-string start-time)
                 "-e" ,(write-to-string end-time)
                 "-o" ,(write-to-string *python-time-offset*)
                 "-m"))
         (proc-var (progn (format t "~A~%" args)
                          (sb-ext:run-program "python3" args :search t :output :stream :wait nil)))
         (output (sb-ext:process-output proc-var)))
    (labels ((read-data (data)
               (let* ((sp (lisp-binary:read-binary 'spectral-point output))
                      (time (spectral-point-ts sp)))
                 ;; Read until we reach end-time or the end of the file (at which point, ts will be 0)
                 (cond ((or (= time 0.0)  ; End of cdip data
                            (and (> time end-time) (not cache-writer)))  ; End of time if we're not caching
                        data)
                       ;; The cdipbuoy.py script won't write data before start-time, so we don't need to handle it
                       (t
                        ;; This is data we want to return. Append it, cache it and keep going
                        (vector-push-extend sp data)
                        (when cache-writer (funcall cache-writer sp))
                        (read-data data))))))
      (format t "Downloading CDIP data for station ~A~%" station-id)
      ;; Metadata will lead the data
      (let ((metadata (lisp-binary:read-binary 'station-metadata output))
            (data (read-data (init-data-vect))))
        (cons data metadata)))))

;;; ========================================== Station Download API ======================================================

(defun download-station (station-id start-time end-time &optional write-cache)
  "Downloads the data and metadata for the given station for all times after start-time and all times before end-time. If
   end-time is nil then all data up to the present momemt will be downloaded.
   that (universal) time will be appended. 

   Either historical, cdip, or RTD data will be downloaded. If historical data is downloaded and end-time is not reached,
   no further data will be downloaded from the RTD url for the station. This is because RTD and historical data do not
   use the same frequencies and are thus too difficult to mix. Therefore, rtd data will need to be downloaded separately.

  Parameters
  ----------
  station-id : string
      the station ID
  start-time : integer
      The start time as a universal time
  end-time : integer
      The end time as a universal time
  write-cache : bool
      If not nil, write all downloaded data to the cache

  Returns
  -------
  A station object with all data between start-time and end-time from either the cdip, historical or rtd data locations.
  nil if no data were found for this station and time range.
"
  (flet ((do-download (download-fn to-rtd)
           (if write-cache
               (with-cache-writer (cache-writer station-id to-rtd)
                 (funcall download-fn station-id start-time end-time #'cache-writer))
               (funcall download-fn station-id start-time end-time))))
         
    (let* ((rtd-data)
           (data-metadata
             (if (is-cdip-station station-id)
                 (do-download #'download-station-cdip nil)
                 (or (do-download #'download-station-hist nil)
                     (progn
                       (setq rtd-data t)
                       (do-download #'download-station-rtd t)))))
           (station (when data-metadata
                      (make-instance
                       'station :id station-id :data (car data-metadata) :metadata (cdr data-metadata)))))

      ;; If write-cache store the station metadata
      (when (and write-cache station)
        (format t "Writing station info to cache")
        (write-station station rtd-data))
      station)))

(defun download-station-metadata (station)
  (setf (metadata station)
        (if (equal (source station) 'cdip)
            (download-cdip-station-metadata (id station))
            (download-noaa-station-metadata (id station)))))
