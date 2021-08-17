import time
import sys
import numpy as np
from netCDF4 import Dataset
from argparse import ArgumentParser

STATION_MAP = {"46267": 248}


class CDIPBuoy(object):

    def __init__(self, url, start_time=0, end_time=None):

        if end_time is None:
            end_time = time.time()

        self._data = Dataset(url, "r")

        self.freqs = np.array(self._data.variables['waveFrequency'])

        # Capture the timestamps and filter by start and end
        self.timestamps = np.array(self._data.variables['waveTime'])
        ts_range = np.where((self.timestamps >= start_time) & (self.timestamps <= end_time))[0]
        self.timestamps = self.timestamps[ts_range]

        # We'll write a single time stamp past end_time so that the caller can tell that we actually reached the end_time
        # and didn't just hit the end of the file
        time_slice = slice(ts_range[0], ts_range[-1] + 1)

        # Need to remove the -999.99 fill values and just set the energy to zero
        # This will then need to be dealt with in the r1 and r2 calculations
        self.c11 = np.array(self._data.variables['waveEnergyDensity'][time_slice])
        self.c11[np.where(self.c11 < 0.0)] = 0.0

        self.a1 = np.array(self._data.variables['waveA1Value'][time_slice])
        self.a2 = np.array(self._data.variables['waveA2Value'][time_slice])
        self.b1 = np.array(self._data.variables['waveB1Value'][time_slice])
        self.b2 = np.array(self._data.variables['waveB2Value'][time_slice])
        self.theta1 = np.array(self._data.variables['waveMeanDirection'][time_slice]) * (np.pi / 180)

        self._a0 = None
        self._r1 = None
        self._r2 = None
        self._theta1 = None
        self._alpha1 = None
        self._theta2 = None
        self._alpha2 = None

    def __bool__(self):
        return len(self.timestamps) > 0

    @property
    def alpha1(self):
        if self._alpha1 is None:
            self._alpha1 = 1.5*np.pi - self.theta1
        return self._alpha1

    @property
    def theta2(self):
        if self._theta2 is None:
            self._theta2 = np.arctan2(self.b2, self.a2)/2
        return self._theta2

    @property
    def alpha2(self):
        if self._alpha2 is None:
            self._alpha2 = 1.5*np.pi - self.theta2
        return self._alpha2

    @property
    def depth(self):
        return float(self._data.variables['metaWaterDepth'][0])

    @property
    def bandwidth(self):
        return np.array(self._data.variables['waveBandwidth'])

    @property
    def lat(self):
        return np.array(self._data.variables['gpsLatitude']).mean()

    @property
    def lon(self):
        return np.array(self._data.variables['gpsLongitude']).mean()

    @property
    def Hs(self):
        """Significant wave hight"""
        return np.array(self._data.variables['waveHs'])

    @property
    def Tp(self):
        """Peak wave period"""
        return np.array(self._data.variables['waveTp'])

    @property
    def Ta(self):
        """Average wave period"""
        return np.array(self._data.variables['waveTa'])

    @property
    def Dp(self):
        """Peak wave direction"""
        return np.array(self._data.variables['waveDp'])

    @property
    def a0(self):
        if self._a0 is None:
            self._a0 = self.c11/np.pi
        return self._a0

    @property
    def r1(self):
        if self._r1 is None:
            self._r1 = np.sqrt(self.a1**2 + self.b1**2)/(self.a0 + 1e-15)
        return self._r1

    @property
    def r2(self):
        if self._r2 is None:
            self._r2 = np.sqrt(self.a2**2 + self.b2**2)/(self.a0 + 1e-15)
        return self._r2

    def Slinear(self, ti, fi, theta):
        a0_2 = self.a0[ti][fi]/2
        return a0_2 + \
            self.a1[ti][fi]*np.cos(theta) + \
            self.b1[ti][fi]*np.sin(theta) + \
            self.a2[ti][fi]*np.cos(theta) + \
            self.b2[ti][fi]*np.sin(theta)

    def Spolar(self, ti, fi, theta):
        return self.c11[ti][fi]*(0.5 +
                                 self.r1[ti][fi]*np.cos(theta - self.theta1[ti][fi]) +
                                 self.r2[ti][fi]*np.cos(2*(theta - self.theta2[ti][fi])))/np.pi

    @property
    def spectral_data(self):
        return zip(self.timestamps, self.c11, self.alpha1, self.alpha2, self.r1, self.r2)

# End Class CDIPBuoy


def writeBinaryInt32(val, stream=sys.stdout):
    stream.buffer.write(np.array([val], dtype=np.int32).tobytes())


def writeBinaryFloat32(val, stream=sys.stdout):
    stream.buffer.write(np.array([val], dtype=np.float32).tobytes())


def writeBinaryCountedArray(ary, stream=sys.stdout):
    stream.buffer.write(np.array(len(ary), dtype=np.uint8).tobytes())
    stream.buffer.write(ary.astype(np.float32).tobytes())


def writeSpectralPoint(ts, c, alpha1, alpha2, r1, r2, stream=sys.stdout):
    writeBinaryInt32(ts, stream)
    writeBinaryCountedArray(c, stream)
    writeBinaryCountedArray(alpha1, stream)
    writeBinaryCountedArray(alpha2, stream)
    writeBinaryCountedArray(r1, stream)
    writeBinaryCountedArray(r2, stream)
    writeBinaryFloat32(9.999, stream)


def writeBuoySpectralData(buoy, start_time=0, end_time=sys.maxsize, stream=sys.stdout):
    for ts, c11, alpha1, alpha2, r1, r2 in buoy.spectral_data:
        writeSpectralPoint(ts, c11, alpha1, alpha2, r1, r2, stream)


def writeBuoyMetadata(buoy, stream=sys.stdout):
    writeBinaryFloat32(buoy.lat)
    writeBinaryFloat32(buoy.lon)
    writeBinaryFloat32(buoy.depth)
    writeBinaryCountedArray(buoy.freqs)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('station', type=str, help="The station ID")
    parser.add_argument('-s', '--start_time', type=int, default=0, help="The time at which to start reading data")
    parser.add_argument('-e', '--end_time', type=int, default=sys.maxsize, help="Time at which to stop reading data")
    parser.add_argument('-m', '--write_meta', action='store_true', default=False,
                        help="Causes the metadata to be written")
    args = parser.parse_args()

    if args.station not in STATION_MAP:
        exit(1)

    cdip_station = STATION_MAP[args.station]
    realtime_url = f'http://thredds.cdip.ucsd.edu/thredds/dodsC/cdip/realtime/{cdip_station}p1_rt.nc'
    historic_url = f'http://thredds.cdip.ucsd.edu/thredds/dodsC/cdip/archive/{cdip_station}p1/{cdip_station}p1_historic.nc'

    for url in (historic_url, realtime_url):
        try:
            buoy = CDIPBuoy(url, args.start_time, args.end_time)
        except OSError:
            continue

        if args.write_meta:
            writeBuoyMetadata(buoy)
        writeBuoySpectralData(buoy, args.start_time, args.end_time)
        sys.exit(0)

        print((f"Failed to retrieve data for station {args.station} ({cdip_station}) in time range: "
               f"[{time.asctime(args.start_time)} -- {time.asctime(args.end_time)}]"), file=sys.stderr)
