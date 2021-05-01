(in-package :wavetools)

(defun message (format &rest values)
  (apply #'format (append `(,*error-output* ,format) values)))
