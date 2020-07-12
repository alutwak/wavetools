
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
  "Ensures that the cache directory exists for the station and returns the path."
  (let ((cache-path (get-station-cache-path station-id)))
    (ensure-directories-exist cache-path)
    cache-path))

(defvar *rtd-path* "rtd/"
  "Path for real-time data cache. This will contain data for the last 45 days. Some of that may overlap with historical data.
In general, the historical data will be better than the real-time data. This data gets updated at least daily, so this cache
is only really useful when multiple hits on the data are done within a few hours.")

(defun get-rtd-data-path (station-id)
  (concatenate 'string (get-station-cache-path station-id) *rtd-path*))

(defun ensure-rtd-data-path-exists (station-id)
  (let ((cache-path (get-rtd-data-path station-id)))
    (ensure-directories-exist cache-path)
    cache-path))

(defvar *hist-path* "hist/"
  "Path for historical data cache.")

(defun this-year ()
  (nth-value 5 (get-decoded-time)))

(defun get-year (time)
  (nth-value 5 (decode-universal-time time)))

(defun get-month (time)
  (values (nth-value 4 (decode-universal-time time))
          (local-time:format-timestring nil (local-time:universal-to-timestamp time) :format '(:short-month))))

(defun get-hist-data-path-date (time)
  (multiple-value-bind (s m h d MM YY)
      (decode-universal-time time)
    (if (= YY (this-year))
        (local-time:format-timestring nil (local-time:universal-to-timestamp time) :format '(:short-month))
        YY)))

(defun get-hist-data-path (station-id &optional month-or-year)
  (format nil "~A~A~A/" (get-station-cache-path station-id) *hist-path* month-or-year))

(defun get-hist-data-path-from-time (station-id time)
  (get-hist-data-path station-id (get-hist-data-path-date time)))

(defun ensure-hist-path-exists (station-id &optional time)
  (let ((cache-path (if time
                        (get-hist-data-path-from-time station-id time)
                        (get-hist-data-path station-id))))
    (ensure-directories-exist cache-path)
    cache-path))

(defun try-get-url (url)
  (handler-case
      (dex:get url)
    (dex:http-request-not-found () nil)))

;; ---------------------------------- Station metadata ------------------------------------

(defstruct (station-metadata (:constructor make-station-metadata (lat lon water-depth)))
  (lat 0.0 :type float :read-only t)
  (lon 0.0 :type float :read-only t)
  (water-depth 0.0 :type float :read-only t))

(defun download-station-metadata (station-id)
  (let* ((loc-scan (ppcre:create-scanner "\\s+([0-9.]+) (N|S) ([0-9.]+) (E|W)"))
         (depth-scan (ppcre:create-scanner "\\s+Water depth: ([0-9.]+) m"))
         (request (try-get-url (format nil "https://www.ndbc.noaa.gov/station_page.php?station=~D&uom=E&tz=STN" station-id)))
         (parsed-req (lquery:$ (lquery:initialize request)))
         (metadata-str (aref (lquery:$ parsed-req "#stn_metadata p" (text)) 0)) ; This comes as an annoying paragraph
         (lat)
         (lon)
         (water-depth))
    (dolist (line (str:lines metadata-str) (make-station-metadata lat lon water-depth))
      (let* ((lat-lon? (ppcre:register-groups-bind ((#'read-from-string lat ltdir lon lndir)) 
                           (loc-scan line)
                         (cons (* lat (if (string= ltdir "S") -1 1)) (* lon (if (string= lndir "E") -1 1)))))
             (w-d? (ppcre:register-groups-bind ((#'read-from-string depth))
                       (depth-scan line)
                     (float depth))))
        (when lat-lon?
          (setq lat (car lat-lon?) lon (cdr lat-lon?)))
        (when w-d? (setq water-depth w-d?))))))

;; ----------------------------------- Raw Data (in string format) -------------------------------

(defstruct (raw-data (:constructor make-raw-data (c a1 a2 r1 r2)))
  (c nil :type stream :read-only t)
  (a1 nil :type stream :read-only t)
  (a2 nil :type stream :read-only t)
  (r1 nil :type stream :read-only t)
  (r2 nil :type stream :read-only t))

(defun raw-data-from-strings (c-data a1-data a2-data r1-data r2-data)
  (make-raw-data
   (make-string-input-stream c-data)
   (make-string-input-stream a1-data)
   (make-string-input-stream a2-data)
   (make-string-input-stream r1-data)
   (make-string-input-stream r2-data)))

(defun raw-data-from-paths (c-path a1-path a2-path r1-path r2-path)
  (make-raw-data
   (open c-path)
   (open a1-path)
   (open a2-path)
   (open r1-path)
   (open r2-path)))

(defun raw-data->list (rd)
  (list (raw-data-c rd) (raw-data-a1 rd) (raw-data-a2 rd) (raw-data-r1 rd) (raw-data-r2 rd)))

(defun raw-data-close (rd)
  (mapc #'close (raw-data->list rd)))

(defmacro with-open-rd-files ((rd c-path a1-path a2-path r1-path r2-path) &body body)
  "Opens the real-time data files from the rtd cache for a station. These files will need to be manually closed."
  `(handler-case
       (let ((,rd (raw-data-from-paths ,c-path ,a1-path ,a2-path ,r1-path ,r2-path)))
         (prog1
             (progn ,@body)
           (raw-data-close ,rd)))
    (file-does-not-exist () nil)))

(defun raw-data-reset (rd)
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
  (not (peek-char nil stream nil nil)))

(defun raw-data-eof-p (rd)
  (some #'eof-p (raw-data->list rd)))

(defvar *date-scan* (ppcre:create-scanner "(\\d{4}) (\\d{2}) (\\d{2}) (\\d{2}) (\\d{2})")
  "Regex for retrieving the date from a buoy data row")

(defun parse-time (row-str)
  (ppcre:register-groups-bind
   ((#'parse-integer year month day hour min))
   (*date-scan*
    row-str)
   (encode-universal-time 0 min hour day month year)))

;;; Real Time Data

(defun parse-rtd (rd &optional start-time end-time)
  "Parses real-time data formatted strings into a list of spectral-points. 

Each parameter comes in a separate file (which are provided to parse-rtd as strings). Each line of each file starts with
a 5-number date, and the values of the parameter for each frequency are given as: <val1> (<freq1>) <val2> (<freq2>) ...
The separation frequency is given in the spec file (which defines the 'c' parameter)."
  (let ((fsep-scan (ppcre:create-scanner "([0-9.]+)"))                                     ; regex for getting the fsep
        (stat-freq-scan (ppcre:create-scanner "([0-9.]+) \\(([0-9.]+)\\)")))           ; regex for getting the stat and freq
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
                     (data)
                     (freqs))
                 (ppcre:do-register-groups ((#'read-from-string stat freq))
                     (stat-freq-scan
                      line
                      nil
                      :start (if c-data? 23 17))
                   (progn
                     (setq data (cons stat data))
                     (setq freqs (cons freq freqs))))
                 (list (reverse data) (reverse freqs) fsep)))

             (parse-all-lines (c-line a1-line a2-line r1-line r2-line time)
               "Parses lines for all raw data streams"
               (let* ((c-f-fsep (parse-line c-line t))
                      (a1 (car (parse-line a1-line nil)))
                      (a2 (car (parse-line a2-line nil)))
                      (r1 (car (parse-line r1-line nil)))
                      (r2 (car (parse-line r2-line nil))))
                 (make-spectral-point
                  time
                  (map 'vector #'make-spect-params ; params
                       (nth 1 c-f-fsep)          ; f
                       (car c-f-fsep)            ; c
                       a1                      
                       a2
                       r1
                       r2)
                  (nth 2 c-f-fsep)))) ; fsep

             (parse-next-entry (parsed)
               "Recursively reads lines from the raw data streams and parses them to create spectral points"
               (if (raw-data-eof-p rd)
                   parsed
                   (multiple-value-bind
                         (c a1 a2 r1 r2)
                       (raw-data-read-line rd)
                     (let ((time (parse-time c)))
                       ;; Remember that rtd comes in reverse time
                       (cond ((and start-time (< time start-time)) ; We passed the start time, so return
                              parsed)
                             ((and end-time (> time end-time))     ; We haven't reached the end time, so keep going
                              (parse-next-entry parsed))
                             (t
                              (parse-next-entry (cons (parse-all-lines c a1 a2 r1 r2 time) parsed)))))))))

      (raw-data-read-line rd) ;; Discard header
      (parse-next-entry '()))))

(defun get-station-rtd-cache-paths (station-id)
  (let ((cache-path (ensure-rtd-data-path-exists station-id)))
        (values
         (str:concat cache-path "c")
         (str:concat cache-path "a1")
         (str:concat cache-path "a2")
         (str:concat cache-path "r1")
         (str:concat cache-path "r2"))))

(defun cache-station-rtd (station-id rd)
  "Saves data to the real-time data cache for the station with station-id."
  (mapc (lambda (data path)
          (str:to-file path (uiop:slurp-stream-string data)))
        (multiple-value-list (get-station-rtd-cache-paths station-id))
        (raw-data->list rd)))

(defun download-station-rtd (station-id &optional start-time end-time cache-data)
  "Downloads the real-time data for the specified station ID"
  (flet ((get-data (path)
           (try-get-url (format nil "https://www.ndbc.noaa.gov/data/realtime2/~D.~A" station-id path))))
    (let ((c-data (get-data "data_spec"))
          (a1-data (get-data "swdir"))
          (a2-data (get-data "swdir2"))
          (r1-data (get-data "swr1"))
          (r2-data (get-data "swr2")))

      (when cache-data
        ;; Do it in a thread so we don't hang this up
        (bt:make-thread
         (lambda ()
           (cache-station-rtd station-id (raw-data-from-strings c-data a1-data a2-data r1-data r2-data)))))

      (parse-rtd (raw-data-from-strings c-data a1-data a2-data r1-data r2-data) start-time end-time))))

;; (defun open-station-rtd-cache (station-id)
;;   "Opens the real-time data files from the rtd cache for a station. These files will need to be manually closed. It's probably
;; better to use with-open-rd-files if you can."
;;   (handler-case
;;       (multiple-value-bind (c a1 a2 r1 r2)
;;           (get-station-rtd-cache-paths station-id)
;;         (raw-data-from-paths c a1 a2 r1 r2))
;;     (file-does-not-exist () nil)))

(defun read-station-rtd-cache (station-id &optional start-time end-time)
  (multiple-value-bind (c a1 a2 r1 r2)
      (get-station-rtd-cache-paths station-id)
    (with-open-rd-files (rd c a1 a2 r1 r2)
      (parse-rtd rd start-time end-time))))

(defun get-raw-data-time (rd)
  "Retrieves the time from the next valid entry in the data. Generally, you will want to use this to check the first (newest)
entry. Returns nil if data is at eof."
  (let* ((c-line (raw-data-read-line rd))
         (time (parse-time c-line)))
    (cond (time time)                     ; Time parsed correctly, return it
          (c-line (get-raw-data-time rd)) ; This must have been a header row, parse next row
          (t nil))))                      ; Reached eof return nil

(defun get-rtd-cache-time (station-id)
  (multiple-value-bind (c a1 a2 r1 r2)
      (get-station-rtd-cache-paths station-id)
    (with-open-rd-files (rd c a1 a2 r1 r2)
      (get-raw-data-time rd))))

;;; Historical Data

;; regex for getting the stat and freq
(defvar *hist-stat-freq-scan* (ppcre:create-scanner "\\s+([0-9.]+)"))

(defun parse-hist (rd &optional start-time end-time)
  "Parses historical data formatted strings into a list of spectral-points. 

Each parameter comes in a separate file (which are provided to parse-rtd as strings). Each line of each file starts with
a 5-number date, and the values of the parameter for each frequency are given as: <val1> <val2> ...
The separation frequency is given in the spec file (which defines the 'c' parameter)."
  (let ((freqs))                                                ; register for frquencies 
    (labels ((parse-line (line)
               "Parses a single line from one of the parameter files."
               (let ((data))
                 (ppcre:do-register-groups ((#'read-from-string stat))
                     (*hist-stat-freq-scan*
                      line
                      nil
                      :start 16)
                   (setq data (cons (float stat) data)))
                 (reverse data)))
             
             (parse-all-lines (c-line a1-line a2-line r1-line r2-line time)
               "Parses lines for all raw data streams"
               (let* ((c (parse-line c-line))
                      (a1 (parse-line a1-line))
                      (a2 (parse-line a2-line))
                      (r1 (parse-line r1-line))
                      (r2 (parse-line r2-line)))
                 (make-spectral-point
                  time
                  (map 'vector #'make-spect-params ; params
                       freqs
                       c
                       a1                      
                       a2
                       r1
                       r2))))

             (parse-next-entry (parsed)
               "Recursively reads lines from the raw data streams and parses them to create spectral points"
               (if (raw-data-eof-p rd)
                   parsed
                   (multiple-value-bind
                         (c a1 a2 r1 r2)
                       (raw-data-read-line rd)
                     (let ((time (parse-time c)))
                       (cond ((and start-time (< time start-time)) ; We haven't reached the start time yet so keep moving
                              (parse-next-entry parsed))
                             ((and end-time (> time end-time))     ; We passed the end time, so return       
                              parsed)
                             (t
                              (parse-next-entry (cons (parse-all-lines c a1 a2 r1 r2 time) parsed)))))))))

      ;; Get the frequencies
      (ppcre:do-register-groups ((#'read-from-string freq))
          (*hist-stat-freq-scan*
           (raw-data-read-line rd)      ; Returns first line of c data and throws away the other data files' lines
           nil
           :start 16)
        (setq freqs (cons freq freqs)))

      ;; Reverse them to put them in order      
      (setq freqs (reverse freqs))

      ;; Return parsed data
      (parse-next-entry '()))))

(defun get-station-hist-cache-paths (station-id time)
  (let ((cache-path (ensure-hist-path-exists station-id time)))
        (values
         (str:concat cache-path "c")
         (str:concat cache-path "a1")
         (str:concat cache-path "a2")
         (str:concat cache-path "r1")
         (str:concat cache-path "r2"))))

(defun cache-station-hist (station-id rd)
  "Saves data to the historical data cache for the station with station-id."
  (let* ((rd-time (get-raw-data-time rd)))
    (raw-data-reset rd)                         ; Rewind the raw data streams after reading the time
    (mapc (lambda (path data)
            (format t "writing to ~A~%" path)
            (str:to-file path (uiop:slurp-stream-string data)))
          (multiple-value-list (get-station-hist-cache-paths station-id rd-time))
          (raw-data->list rd))))


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

(defun download-station-hist (station-id start-time &optional end-time cache-data)
  "Historical data is kept in yearly chunks for each year up to the current one. For the current year, data is kept in monthly
chunks. The url format for each is slightly different, so we need to choose the url based on whether time is from this year
\(at the time that this function is called\) or from a previous year.

download-station-hist will download all data for the year/month of the start-time parameter, and it will parse the data until
the end-time or the end of the data is reached.

If cache-data is non-nil, the downloaded data will be cached locally for future use."
  (labels ((get-year-data (char-key path)
             "Gets data at a 'year' url"
             (try-get-url
              (format
               nil
               "https://www.ndbc.noaa.gov/view_text_file.php?filename=~D~C~D.txt.gz&dir=data/historical/~A/"
               station-id
               char-key
               (get-year start-time)
               path)))
           (get-month-data (path)
             (or (try-get-url (funcall (car *hist-month-urls*) station-id path start-time))
                 (try-get-url (funcall (cadr *hist-month-urls*) station-id path start-time))))
           (get-data (char-key path)
             (if (= (get-year start-time) (this-year))
                 (get-month-data path)
                 (get-year-data char-key path))))
    (let ((data `(,(get-data #\w "swden")
                   ,(get-data #\d "swdir")
                   ,(get-data #\i "swdir2")
                   ,(get-data #\j "swr1")
                   ,(get-data #\k "swr2"))))

      (when (every #'stringp data)
        (when cache-data
          ;; Do it in a thread so we don't hang this up
          (bt:make-thread
           (lambda ()
             (cache-station-hist station-id (apply #'raw-data-from-strings data)))))

        (parse-hist (apply #'raw-data-from-strings data) start-time end-time)))))

(defun read-station-hist-cache (station-id start-time &optional end-time)
  (multiple-value-bind (c a1 a2 r1 r2)
      (get-station-hist-cache-paths station-id start-time)
    (with-open-rd-files (rd c a1 a2 r1 r2)
      (parse-hist rd start-time end-time))))

(defun get-hist-data (station-id start-time end-time &optional cache-new-data)
  (labels ((get-data (start)
             (format t
                     "start: ~A~%"
                     (local-time:format-timestring nil (local-time:universal-to-timestamp start)
                                                   :format local-time:+asctime-format+))
             (or (read-station-hist-cache station-id start end-time)
                 (download-station-hist station-id start end-time cache-new-data))))
    (do* ((next-data (get-data start-time) (get-data next-start))
          (data next-data (append next-data data))
          (next-start))
         ((not next-data) (reverse data)) ; We're done when next-data is empty. This is sloppy and inefficient, but it works
      
      ;; Make sure that the next start time is at the beginning of the next hist file
      ;; This will be the beginning of the next month in all cases
      (let* ((last-end (spectral-point-ts (car next-data))))
        (format t
                     "last-end: ~A~%"
                     (local-time:format-timestring nil (local-time:universal-to-timestamp last-end)
                                                   :format local-time:+asctime-format+))
        (setq next-start
              (multiple-value-bind (s m h d mon yr) (decode-universal-time last-end)
                (let* ((mon (1+ mon))
                       (yr (if (> mon 12) (1+ yr) yr)))
                  (encode-universal-time 0 0 0 1 (1+ (mod (1- mon) 12)) yr))))))))

;; General Data Retrieval API

