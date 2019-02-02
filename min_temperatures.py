"""
Run with:

python min_temperatures.py assets/1800.csv
"""

from mrjob.job import MRJob

class MRMinTemperature(MRJob):

    def mapper(self, _, line):
        (location, timestamp, temp_type, data, x, y, z, w) = line.split(',')
        if (temp_type == 'TMIN'):
            temperature = float(data) / 10.0
            yield location, temperature

    def reducer(self, location, temps):
        yield location, min(temps)


if __name__ == '__main__':
    MRMinTemperature.run()
