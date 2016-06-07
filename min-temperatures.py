from mrjob.job import MRJob


class MRMinTemperatures(MRJob):
    """
    Temperature extremes in the 1800
    * What is the min temperature seen for each weather station?
    * What is the maximum?
    """

    def make_celsius(self, tenths_of_celsius):
        celsius = float(tenths_of_celsius)/10.0
        return celsius

    def mapper(self, _, line):
        (station, date, maxmin, data, x, y, z, w) = line.split(',')
        # map to find key/value pairs for min temp
        if maxmin == 'TMIN':
            temperature = self.make_celsius(data)
            yield station, temperature

    def reducer(self, station, temps):
        yield station, min(temps)


if __name__ == '__main__':
    MRMinTemperatures.run()
