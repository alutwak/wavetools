(asdf:defsystem :wavetools
  :author "Ayal Lutwak"
  :version "0.0.1"
  :description "A colletion of tools for analyzing wave data for the purpose of surf prediction"
  :licence "Public Domain / 0-clause MIT"
  :long-description
  "Wavetools is a library for wave analysis."
  :class :package-inferred-system
  :pathname "src/"
  :depends-on (:wavetools/all)
  :components ((:static-file "py/cdipbuoy.py")))
