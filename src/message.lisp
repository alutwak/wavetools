(defpackage :wavetools/message
  (:use :cl)
  (:nicknames :message)
  (:export :message))

(in-package :wavetools/message)

(defun message (format &rest values)
  (apply #'format (append `(,*error-output* ,format) values)))
