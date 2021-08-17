(setf *compile-verbose* nil)

(let ((quicklisp-init (merge-pathnames "quicklisp/setup.lisp"
                                       (user-homedir-pathname))))
  (when (probe-file quicklisp-init)
    (load quicklisp-init)))

(defvar *this-dir* (directory-namestring *load-pathname*))

(let ((this-dir (directory-namestring *load-pathname*)))
  (setq asdf:*central-registry* (cons this-dir asdf:*central-registry*)))

(handler-bind ((warning
                 #'(lambda (c)
                     (invoke-restart 'muffle-warning))))
  (asdf:load-system "wavetools" :silent t))

(ql:quickload :unix-opts :silent t)
(ql:quickload :cl-ppcre :silent t)

(defun parse-date-time (arg)
  (ppcre:register-groups-bind
      ((#'read-from-string month day year hour min))
      ("([0-9]+)-([0-9]+)-([0-9]+) ([0-9]+):([0-9]+)" arg)
    (encode-universal-time 0 min hour day month year)))

(opts:define-opts
  (:name :help
   :description "Print this help message"
   :short #\h
   :long "help")
  (:name :station
   :description "The station name to use"
   :short #\s
   :long "station"
   :arg-parser #'identity)
  (:name :date-time
   :description "The date and time with format \"MM-DD-YY HH:MM\""
   :short #\d
   :long "date-time"
   :arg-parser #'parse-date-time))

(defvar *required-args* '(:station :date-time))

(defvar *args* (opts:get-opts))

(when (or (getf *args* :help))
  (opts:describe)
  (quit))

(mapc
 (lambda (arg)
   (when (not (getf *args* arg))
     (opts:describe)
     (format *error-output* "~S parameter is required~%" arg)     
     (quit)))
 *required-args*)

(let* ((station-id (getf *args* :station))
       (time (getf *args* :date-time))
       (station (wavetools:get-station-at-time station-id time t)))
  (if station
      (wavetools:dump-station station 360)
      (format *error-output* "Failed to retrieve data for station~%")))

