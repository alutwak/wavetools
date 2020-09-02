(defpackage :wavetools/station-download
  (:use :cl :sb-ext :wavetools/wavespectrum :wavetools/station)
  (:import-from :lisp-binary
                :write-binary
                :with-open-binary-file)
  (:import-from :cl-ppcre
                :do-register-groups
                :create-scanner
                :register-groups-bind)
  (:import-from :dexador
                :http-request-not-found)
  (:import-from :lquery
                :$
                :initialize)
  (:export :download-station-data
           :download-station-metadata
           :download-station-rtd))

(in-package :wavetools/station-download)

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

(defun deg-to-rad (deg)
  (/ (* pi deg) 180.0))

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
    (encode-universal-time 0 min hour day month year 0)))


;;; ------------------------------------------ Metadata ==================================================================

(defun download-noaa-station-metadata (station-id)
  "Downloads and returns the metadata for the given station id"
  (handler-case
      (let* ((loc-scan (ppcre:create-scanner "\\s+([0-9.]+) (N|S) ([0-9.]+) (E|W)"))
             (depth-scan (ppcre:create-scanner "\\s+Water depth: ([0-9.]+) m"))
             (request (dex:get (format nil "https://www.ndbc.noaa.gov/station_page.php?station=~D&uom=E&tz=STN" station-id)))
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
            (when w-d? (setq water-depth w-d?)))))
    (dex:http-request-not-found () nil)))

(defun download-cdip-station-metadata (station-id)
  "Downloads the metadata for a cdip station. This function is very inefficient because it downloads the data for the entire
   history of the station just to get the metadata. download-station-cdip will automatically download the metadata, so you
   don't need to call this function unless you just want to get the metadata and no other data."
  (let* ((args (format nil "cdipbuoy.py ~D -s 1 -e 0 -m" station-id)) ; Start ends 
         (proc-var (run-program "python" args :search t :output :stream :wait nil))
         (output (process-output proc-var)))
    (lisp-binary:read-binary 'station-metadata output)))

;;; ========================================= Historical Data ==========================================

;; regex for getting the stat and freq
(defvar *hist-stat-freq-scan* (ppcre:create-scanner "\\s+([0-9.]+)"))

(defun parse-hist (station rd &optional start-time end-time cache-writer)
  "Parses historical data formatted strings into the data argument, which must be an adjustable vector. Only data points with
  time stamps between start-time and end-time will be station-pushed into the data vector. If cache-stream is not nil, it
  must be a binary stream and all data points will be written to that stream.

  Each parameter comes in a separate stream. Each line of each stream starts with a 5-number date, followed by a series of
  space-separated floating point values. The header for each file contains the set of frequencies, one for each row of data.

  Returns 'eot if end-time was reached and 'eof otherwise."
  (labels ((parse-line (line &optional (mult 1.0))
             "Parses a single line from one of the parameter files."
             ;(format t "~A~%" line)
             (let ((data (make-array (spoint-size station) :element-type 'float :initial-element 0.0))
                   (dref -1))
               (ppcre:do-register-groups ((#'read-from-string stat))
                   (*hist-stat-freq-scan*
                    line
                    nil
                    :start 16)
                 (setf (aref data (incf dref)) (* stat mult)))
               data))
           
           (parse-all-lines (c-line a1-line a2-line r1-line r2-line time)
             "Parses lines for all raw data streams"
             (let* ((c (parse-line c-line))
                    (a1 (map 'vector #'deg-to-rad (parse-line a1-line)))
                    (a2 (map 'vector #'deg-to-rad (parse-line a2-line)))
                    (r1 (parse-line r1-line 0.01))      ;; r1 and r2 values are scaled by 100 in historical data
                    (r2 (parse-line r2-line 0.01)))     ;; We need to unscale them
               (make-spect-point time c a1 a2 r1 r2)))

           (parse-next-entry (reached-end)
             "Recursively reads lines from the raw data streams and parses them to create spectral points"
             (multiple-value-bind
                   (c a1 a2 r1 r2)
                 (raw-data-read-line rd)
               (let* ((time (parse-time c))
                      (sp (parse-all-lines c a1 a2 r1 r2 time))
                      (end-status (or reached-end (when (and end-time (> time end-time)) 'eot))))
                 (when cache-writer (funcall cache-writer sp))
                 (unless (or (and start-time (< time start-time)) end-status)
                   (station-push station sp))
                 (cond ((raw-data-eof-p rd) ;; We're done parsing so return 
                        (or reached-end 'eof)) ;; return reached-end if it's 'eot, otherwise return 'eof
                       (t ;; Keep going
                        (parse-next-entry end-status)))))))

    
    (if (not (freqs-defined station))
        
        ;; Get the frequencies when we don't have them
        (let ((freqs))
          (ppcre:do-register-groups ((#'read-from-string freq))
              (*hist-stat-freq-scan*
               (raw-data-read-line rd)  ; Returns first line of c data and throws away the other data files' lines
               nil
               :start 16)
            (setq freqs (cons freq freqs)))
          
          ;; Reverse them to put them in order      
          (setf (freqs station) (apply #'vector (reverse freqs))))

        ;; Otherwise, just throw away the header
        (raw-data-read-line rd))

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

(defun download-station-hist-next-chunk (station start-time &optional end-time cache-writer)
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
             (dex:get
              (format
               nil
               "https://www.ndbc.noaa.gov/view_text_file.php?filename=~D~C~D.txt.gz&dir=data/historical/~A/"
               (id station)
               char-key
               (get-year start-time)
               path)))
           (get-month-data (path)
             (or (try-get-url (funcall (car *hist-month-urls*) (id station) path start-time))
                 (dex:get (funcall (cadr *hist-month-urls*) (id station) path start-time))))
           (get-data (char-key path)
             (if (= (get-year start-time) (this-year))
                 (get-month-data path)
                 (get-year-data char-key path))))

    ;; Get the raw data and parse it
    (handler-case
        (let ((raw-data (mapcar #'get-data '(#\w #\d #\i #\j #\k) '("swden" "swdir" "swdir2" "swr1" "swr2"))))
          (parse-hist station (apply #'raw-data-from-strings raw-data) start-time end-time cache-writer))

      ;; If the data don't exist for this time, return nil
      (dex:http-request-not-found () nil))))

(defun download-station-hist (station start-time end-time &optional cache-writer)
  (labels ((calculate-next-start (last-start)
             (multiple-value-bind (s m h d mon yr) (decode-universal-time last-start 0)
               (let* ((mon (if (= yr (this-year))
                               (1+ (mod mon 12))  ;; This year, we only increment by months
                               1))                ;; Previous years, we increment by years
                      (yr (if (= mon 1)
                              (1+ yr)
                              yr)))
                 (encode-universal-time 0 0 0 1 mon yr 0))))
           (download-all (next-start)
             (multiple-value-bind (s m h d mon yr) (decode-universal-time next-start 0)
               (format t "~D-~D-~D ~D:~D~%"
                         mon d yr h m))
             (let ((end-reached (download-station-hist-next-chunk station next-start end-time cache-writer)))
               (cond ((equal end-reached 'eot)
                      ;; We've reached end-time
                      (format t "All requested data found ~%")
                      'eot)
                     ((and (not end-reached) (= (get-year next-start) (this-year)))
                      ;; No data were found and we're in this year, so there's no newer data to be found
                      (format t "All requested data not found ~%")
                      'eof)
                     (t
                      ;; Either data were found, or they weren't but there may be later data. Either way, keep searching
                      (download-all (calculate-next-start next-start)))))))
    (format t "Searching for historical data for station ~D~%" (id station))
    (download-all start-time)))

;;; ======================================== Real Time Data ========================================================

(defun parse-rtd (station rd &optional start-time end-time cache-writer)
  "Parses real-time data formatted strings into the data argument, which must be an adjustable vector. Only data points with
  time stamps between start-time and end-time will be station-pushed into the data vector. If cache-stream is not nil, it must be a
  binary stream and all data points will be written to that stream.

  Each parameter comes in a separate file (which are provided to parse-rtd as strings). Each line of each file starts with a
  5-number date, and the values of the parameter for each frequency are given as: <val1> (<freq1>) <val2> (<freq2>) ...  The
  separation frequency is given in the spec file (which defines the 'c' parameter).

  Returns 'eot if end-time was reached and 'eof otherwise"
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

        (raw-data-reset rd)) ;; Reset the data

      ;; Now, parse the data
      (raw-data-read-line rd) ;; Discard header

      ;; RT data comes in reverse time order, so we need to cons each line and then map back over to put it in order
      (let ((parsed (parse-next-entry '())))
        (reduce (lambda (reached-end sp)
                  
                  ;; Write everything to the cache when it's there
                  (when cache-writer (funcall cache-writer sp 'rtd))

                  ;; Only return data that's between the start and end times when they're given
                  (let ((time (spectral-point-ts sp)))
                    (cond ((and start-time (< time start-time))
                           'eof)
                          ((and end-time (> time end-time))
                           (break)
                           'eot)
                          (t
                           (station-push station sp)
                           'eof))))
                parsed)))))

(defun download-station-rtd (station &optional start-time end-time cache-writer)
  "Downloads the real-time data for the specified station id, and appends it to the data argument, which must be an
  adjustable vector. If start-time is non-nil, then only data points after that \(universal\) time will be appended. If
  end-time is non-nil then only data points before that time will be appended. If cache-data is non-nil, the all downloaded
  data will saved in the data cache.

  Returns 'eot value if end-time was reached, 'eof if it wasn't, and nil if the data were not reachable on the server."
  (flet ((get-data (path)
           (dex:get (format nil "https://www.ndbc.noaa.gov/data/realtime2/~D.~A" (id station) path))))
    (format t "Downlading real-time-data for station ~D~%" (id station))
    (handler-case
        (let ((raw-data (mapcar #'get-data '("data_spec" "swdir" "swdir2" "swr1" "swr2"))))

          ;; Parse using cache file if we're caching, and without it if we're not
          (parse-rtd station (apply #'raw-data-from-strings raw-data) start-time end-time cache-writer))
      (dex:http-request-not-found () nil))))

;;; ========================================== CDIP Stations ========================================================

(defvar *python-time-offset* (encode-universal-time 0 0 0 1 1 1970 0))

(defun download-station-cdip (station start-time end-time &optional cache-writer)
  (let* ((args (append `("cdipbuoy.py"
                         ,(write-to-string (id station))
                         "-s" ,(write-to-string start-time)
                         "-e" ,(write-to-string end-time)
                         "-o" ,(write-to-string *python-time-offset*))
                       (if (not (freqs-defined station)) '("-m") '())))
         (proc-var (progn (format t "~A~%" args)
                     (run-program "python" args :search t :output :stream :wait nil)))
         (output (process-output proc-var)))
      (labels ((read-data ()
              (let* ((sp (lisp-binary:read-binary 'spectral-point output))
                     (time (spectral-point-ts sp)))
                ;; Read until we reach end-time or the end of the file (at which point, ts will be 0)
                (cond ((and (> time end-time) (not cache-writer))
                       ;; We reached the end time (but only if we're not caching)
                       'eot)
                      ((= time 0.0)
                       ;; We've reached the end of the file. Return 'eot if we hit it, otherwise 'eof
                       (if (> time end-time)
                           'eot
                           'eof))
                      ;; The cdipbuoy.py script won't write data before start-time, so we don't need to handle it
                      (t
                       ;; This is data we want to return. Append it, cache it and keep going
                       (station-push station sp)
                       (when cache-writer (funcall cache-writer sp))
                       (read-data))))))
        (format t "Downloading CDIP data for station ~D~%" (id station))
        (when (not (metadata-defined station))
          ;; Metadata will lead the data
          (let ((metadata (lisp-binary:read-binary 'station-metadata output)))
            (setf (metadata station) metadata)))
        (read-data))))

;;; ========================================== Station Download API ======================================================

(defun download-station-data (station start-time end-time &optional cache-writer)
  "Downloads the data for the given station, which must already have an id, and appends it to the station's data. If
   start-time is non-nil, then only data points after that \(universal\) time will be appended. If end-time is non-nil then
   only data points before that time will be appended. If cache-writer is non-nil, then it must be a function and it will 
   be called with each spectral point downloaded.

   Only historical (or cdip) data will be downloaded. Real-time data may be reported with a different set of frequencies,
   making it difficult to mix historical and real-time data. I don't want to deal with this right now, so for now, I'm
   keeping it separate. If you want data that's more recent than the latest historical data, you'll need to download it
   into a separate station object.

   Returns 'eot value if end-time was reached, 'eof if it wasn't, and nil if no data were found."
  (if (equal (source station) 'cdip)
      (download-station-cdip station start-time end-time cache-writer)
      (download-station-hist station start-time end-time cache-writer)))

(defun download-station-metadata (station)
  (setf (metadata station)
        (if (equal (source station) 'cdip)
            (download-cdip-station-metadata (id station))
            (download-noaa-station-metadata (id station)))))
