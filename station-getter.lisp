
(in-package :wavetools)

(defun get-station-data (station-id start-time end-time &optional write-cache)
  "Returns the station data for the given time range, if it exists, by:
   1. Searching for it in the cache
   2. Attempting to download it from the noaa or cdip servers

   If not all data were found in the cache then all of the data will be downloaded, even if only one record
   is missing. This is fine for short time ranges, in which case the chances are good that we'd be downloading
   and reparsing all of the data anyway. For long time ranges this is not ideal, but this will happen rarely and
   only once for any given time range.

   Note: This does not check to see if there were time gaps internally in the cache. Again, this is probably safe
         for shorter time ranges, but there's definitely a bug here if this happens. But I don't want to deal with
         that yet, so I'll save it for some future day if it ever becomes a problem.

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
  (let ((station (read-cache station-id start-time end-time)))
    (if (or (not station)                             ; No data at all
             (> (- end-time (end station)) 3600)      ; More than 1 hour gap at the end
             (> (- (begin station) start-time) 3600)) ; More than 1 hour gap at the beginning

        ;; If we didn't get any data, or we didn't get all data then download from the servers
        (download-station station-id start-time end-time write-cache)

        ;; Otherwise, return the station
        station)))


(defun get-station-at-time (station-id time &optional write-cache)
  " Retrieves the spectral point for the given station at the time closest to the given time.

  Parameters
  ----------
  station-id : string
      the station ID
  time : integer
      The time as a universal time
  write-cache : bool
      If not nil, write all downloaded data to the cache

  Returns
  -------
  A station object with a single spectral point from a time as close to the given time as possible.
  nil if no data were found for this station within an hour of the given time.
"
  (let* ((start-time (- time 3600))
         (end-time (+ time 3600))
         (station (get-station-data station-id start-time end-time write-cache)))
    (when (and station (> (length (data station)) 0))
      (let* ((time-of-closest
               (cdr
                (reduce

                 ;; Reduces to the minimum time
                 (lambda (dt-sp-a dt-sp-b)
                   (let ((dt-a (car dt-sp-a))
                         (dt-b (car dt-sp-b)))
                     (if (< dt-a dt-b)
                         dt-sp-a
                         dt-sp-b)))

                 ;; Extracts the time difference between the given time and the spectral point
                 (map 'list
                      (lambda (sp)
                        (let ((ts (spectral-point-ts sp)))
                          (cons (abs (- time ts)) ts)))
                      (data station))))))
        (setf (data station)
              (delete-if-not (lambda (sp) (= (spectral-point-ts sp) time-of-closest)) (data station)))
        station))))
