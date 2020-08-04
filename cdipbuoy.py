
import sys
import numpy as np
from netCDF4 import Dataset
from argparse import ArgumentParser

STATION_MAP = {46267: 248}


class CDIPBuoy(object):

    def __init__(self, url):
        self._data = Dataset(url, "r")
        self._a0 = None
        self._r1 = None
        self._r2 = None
        self._theta1 = None
        self._alpha1 = None
        self._theta2 = None
        self._alpha2 = None

    @property
    def freqs(self):
        return np.array(self._data.variables['waveFrequency'])

    @property
    def timestamps(self):
        return np.array(self._data.variables['waveTime'])

    @property
    def c11(self):
        return np.array(self._data.variables['waveEnergyDensity'])

    @property
    def a1(self):
        return np.array(self._data.variables['waveA1Value'])

    @property
    def a2(self):
        return np.array(self._data.variables['waveA2Value'])

    @property
    def b1(self):
        return np.array(self._data.variables['waveB1Value'])

    @property
    def b2(self):
        return np.array(self._data.variables['waveB2Value'])

    @property
    def theta1(self):
        if self._theta1 is None:
            self._theta1 = np.array(self._data.variables['waveMeanDirection'])*(np.pi/180)
        return self._theta1

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
            self._r1 = np.sqrt(self.a1**2 + self.b1**2)/self.a0
        return self._r1

    @property
    def r2(self):
        if self._r2 is None:
            self._r2 = np.sqrt(self.a2**2 + self.b2**2)/self.a0
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


def writeBuoySpectralData(buoy, start_time=0, end_time=sys.maxsize, offset=0, stream=sys.stdout):
    start_time -= offset
    end_time -= offset
    for ts, c11, alpha1, alpha2, r1, r2 in buoy.spectral_data:
        if ts > end_time:
            # We'll write a single time stamp past end_time so that the caller can tell that we actually reached the end_time
            # and didn't just hit the end of the file
            writeSpectralPoint(ts+offset, c11, alpha1, alpha2, r1, r2, stream)
            return
        elif ts >= start_time:
            writeSpectralPoint(ts+offset, c11, alpha1, alpha2, r1, r2, stream)


def writeBuoyFrequencies(buoy, stream=sys.stdout):
    writeBinaryCountedArray(buoy.freqs)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('station', type=int, help="The station ID")
    parser.add_argument('-s', '--start_time', type=int, default=0, help="The time at which to start reading data")
    parser.add_argument('-e', '--end_time', type=int, default=sys.maxsize, help="Time at which to stop reading data")
    parser.add_argument('-o', '--time_offset', type=int, default=0, help="Offset time")
    parser.add_argument('-f', '--write_freqs', action='store_true', default=False,
                        help="Causes the frequencies to be written")
    args = parser.parse_args()

    if args.station not in STATION_MAP:
        exit(1)

    url = f'http://thredds.cdip.ucsd.edu/thredds/dodsC/cdip/realtime/{STATION_MAP[args.station]}p1_rt.nc'
    buoy = CDIPBuoy(url)
    if args.write_freqs:
        writeBuoyFrequencies(buoy)
    writeBuoySpectralData(buoy, args.start_time, args.end_time, args.time_offset)
