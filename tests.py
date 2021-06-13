from traffic_accidents_pipe import Pipeline
import unittest
import pandas as pd
from unittest import mock
from unittest.mock import Mock, MagicMock


class Test_pipeline(unittest.TestCase):

    def accidents(self):
        data = [('200501BS00001', 'Serious', '2005-01-04', 'Tuesday', '17:42'), 
                ('200501BS00002', 'Slight', '2005-01-05', 'Wednesday', '17:36'), 
                ('200501BS00003', 'Slight', '2005-01-06', 'Thursday', '00:15'), 
                ('200501BS00004', 'Slight', '2005-01-07', 'Friday', '10:35'), 
                ('200501BS00005', 'Slight', '2005-01-10', 'Monday', '21:13')]
        colnames = ['accident_index', 'accident_severity', 'date', 'day_of_week', 'time']

        return pd.DataFrame(data, columns=colnames) 


    def vehicles(self):
        data = [('200501BS00001', '26 - 35', 3.0, 'Urban area', 'Data missing or out of range'), 
                ('200501BS00002', '26 - 35', None, 'Urban area', 'Data missing or out of range'), 
                ('200501BS00003', '26 - 35', 4.0, 'Data missing or out of range', 'Data missing or out of range'), 
                ('200501BS00004', '66 - 75', None, 'Data missing or out of range', 'Data missing or out of range'), 
                ('200401BS00004', '26 - 35', 1.0, 'Urban area', 'Data missing or out of range')]

        colnames = ['accident_index', 'age_band_of_driver', 'age_of_vehicle', 'driver_home_area_type', 'journey_purpose_of_driver']

        return pd.DataFrame(data, columns=colnames) 


    def merged(self):
        data = [('200401BS00001', 'Slight', '2005-01-05', 'Wednesday', '36 - 45', 3.0, 'Data missing or out of range', 'Journey as part of work'), 
                ('200401BS00002', 'Slight', '2005-01-06', 'Thursday', '26 - 35', 5.0, 'Urban area', 'Journey as part of work'), 
                ('200401BS00003', 'Slight', '2005-01-07', 'Friday', '46 - 55', 4.0, 'Urban area', 'Other/Not known (2005-10)'), 
                ('200501BS00005', 'Slight', '2005-01-10', 'Monday', '46 - 55', 10.0, 'Data missing or out of range', 'Other/Not known (2005-10)'), 
                ('200501BS00006', 'Slight', '2005-01-11', 'Tuesday', '46 - 55', 1.0, 'Urban area', 'Other/Not known (2005-10)'), 
                ('200501BS00006', 'Slight', '2005-01-11', 'Tuesday', '26 - 35', 2.0, 'Urban area', 'Other/Not known (2005-10)')]

        colnames = ['accident_index', 'accident_severity', 'date', 'day_of_week', 'age_band_of_driver', 'age_of_vehicle', 'driver_home_area_type', 'journey_purpose_of_driver']

        return pd.DataFrame(data, columns=colnames) 


    def test_merged_data(self):
        pipe = Pipeline()
        df = pipe.merge_data(self.accidents(), self.vehicles())
        result = list(df.columns)
        expected_result = list(self.merged().columns)

        self.assertCountEqual(result, expected_result)
        self.assertListEqual(sorted(result), sorted(expected_result))
        self.assertEqual(df.shape[0], 4)


if __name__ == '__main__':
    unittest.main()