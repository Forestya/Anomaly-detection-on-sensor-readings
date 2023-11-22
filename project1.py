from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env

class proj1(MRJob):   
    # This class is a MapReduce job that processes sensor data

    def mapper_init(self):
        # This function initializes the mapper with empty dictionaries for daily sum and count
        self.daily_sum = {}
        self.daily_count = {}

    def mapper(self, _, line):
        # This function processes each line from the input
        # It splits the line into sensor, date, and humidity
        # It then updates the daily sum and count for each sensor
        sensor, date, humidity = line.strip().split(',')
        daily_key = sensor
        daily_value = f'{date}\t{humidity}'
        self.daily_sum.setdefault(daily_key, [])
        self.daily_sum[daily_key].append(daily_value)
        self.daily_count[daily_key] = self.daily_count.get(daily_key, 0) + 1
       

    def mapper_final(self):
        # This function yields the daily sum for each sensor
        for d_key, d_value in self.daily_sum.items():
            yield d_key, d_value
    
    def reducer(self, key, values):
        # This function calculates the average humidity for each sensor and each day
        # It then checks if the difference between the overall average and the daily average exceeds a threshold
        tau = jobconf_from_env('myjob.settings.tau')
        daily_data = {}
        daily_count = {}
        overall_data = {}
        overall_count = {}
        for value in values:
            for v in value:
                date, humidity = v.split('\t')
                d_key = f'{key}\t{date}'
                overall_data[key] = overall_data.get(key, 0) + float(humidity)
                overall_count[key] = overall_count.get(key, 0) + 1
                daily_data[d_key] = daily_data.get(d_key, 0) + float(humidity)
                daily_count[d_key] = daily_count.get(d_key, 0) + 1
        
        for d_sum in daily_data.keys():
            sensor, date = d_sum.split('\t')
            overall_avg = overall_data[sensor] / overall_count[sensor]
            daily_avg = daily_data[d_sum] / daily_count[d_sum]
            gap_value = abs(overall_avg - daily_avg)
            if gap_value > float(tau):
                yield sensor, f'{date},{gap_value}'
           
    SORT_VALUES = True

    def steps(self):
        # This function defines the steps of the MapReduce job
        JOBCONF = {
            'mapreduce.map.output.key.field.separator': ',',
            'mapreduce.partition.keypartitioner.options':'-k1,2', 
            'mapreduce.job.output.key.comparator.class':'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',    
            'mapreduce.partition.keycomparator.options':'-k1,1 -k2,2nr'
        }
        return [MRStep(mapper_init=self.mapper_init, mapper=self.mapper, mapper_final=self.mapper_final, reducer=self.reducer)]
    

if __name__ == '__main__':
    proj1.run()
