import pandas as pd
import numpy as np
from math import sqrt, isnan

from app.analysis.analysis_utils import AnalysisUtility


class FindStepsFromTimeSeries(AnalysisUtility):
    def __init__(self):
        self.utility_name = 'find_steps_from_time_series'
        self.utility_description = 'Finds steps from a time series data using a wavelet transform multiscale product'
        self.utility_parameters = [
            {
                'parameter_name': 'step_threshold',
                'parameter_description': 'Not yet written',
                'parameter_type': 'float',
                'parameter_default': None,
                'parameter_is_required': False
            },
            {
                'parameter_name': 'column_name',
                'parameter_description': 'Not yet written',
                'parameter_type': 'string',
                'parameter_default': None,
                'parameter_is_required': False
            }
        ]
        self.input_type = 'time_series'
        self.output_type = 'step_list'
        super(FindStepsFromTimeSeries, self).__init__()

    async def call(self, task):
        column_name = task.utility_parameters.get('column_name')
        step_threshold = task.utility_parameters.get('step_threshold')
        
        input_data, filled_in = self.prepare_timeseries(self.input_data['absolute_counts'])
        column_steps = []
        interestingness = []
        if column_name:
            columns = [column_name]
        else:
            columns = input_data.columns
        for column in columns:
            data = input_data[column]
            prod, _ = self.mz_fwt(data, 3)
            step_indices = self.find_steps(prod, step_threshold)
            step_sizes, errors = self.get_step_sizes(input_data[column], step_indices)
            step_times = [input_data.index[idx].year for idx in step_indices]
            step_keys = ['step_time', 'step_start', 'step_end', 'step_error']
            column_steps.append({
                'column': column,
                'steps': [dict(zip(step_keys, [item[0], *item[1], item[2]])) for item in (zip(step_times, step_sizes, errors))]
            })
            interestingness.append({
                'column': column,
                'steps': [abs(prod[step_idx]) for step_idx in step_indices]
            })
        # TODO: Implement interestingness values
        return {'result': column_steps,
                'interestingness': interestingness}

    @staticmethod
    def prepare_timeseries(ts, fill_na='interpolate'):
        """
        Prepares a time series in  a dictionary format into a pandas timeframe, adding missing values where necessary.
        :param ts: a timeseries returned by Corpus.timeseries()
        :param fill_na: method used for filling missing information. Different available options are:
                        'none': do not fill missing values
                        'zero': replace missing values with zeroes
                        'interpolate': use the pandas.Dataframe.interpolate(method='linear') for missing values inside
                        valid values and interpolate('pad') outside valid values
        :return: two DataFrames. df contains the time series, filled_values shows which values were filled in
        """
        if fill_na not in ['none', 'interpolate', 'zero']:
            raise ValueError("Invalid value for parameter fill_na. Valid values are 'none', 'zero' and 'interpolate'")
        df = pd.DataFrame(ts)
        first_date = df.index[0].split('-')
        if len(first_date) == 1:
            freq = 'AS'
        elif len(first_date) == 2:
            freq = 'MS'
        elif len(first_date) == 3:
            freq = 'D'
        else:
            raise ValueError('Invalid date format in the time series! Use YYYY or YYYY-MM or YYYY-MM-DD!')
        df.index = pd.to_datetime(df.index)
        if len(df.index) < 2:
            return df

        idx = pd.date_range(start=df.index.min(), end=df.index.max(), freq=freq)
        df = df.reindex(idx)
        filled_values = df.isna
        if fill_na == 'zero':
            df = df.fillna(0)
        elif fill_na == 'interpolate':
            df = df.interpolate(limit_direction='both')

        return df, filled_values

    def mz_fwt(self, x, n=3):
        """
        A modified version of the code at https://github.com/thomasbkahn/step-detect
        (There were two off-by-one error in the original code that caused the wavelets to drift left)

        Computes the multiscale product of the Mallat-Zhong discrete forward
        wavelet transform up to and including scale n for the input data x.
        If n is even, the spikes in the signal will be positive. If n is odd
        the spikes will match the polarity of the step (positive for steps
        up, negative for steps down).
        This function is essentially a direct translation of the MATLAB code
        provided by Sadler and Swami in section A.4 of the following:
        http://www.dtic.mil/dtic/tr/fulltext/u2/a351960.pdf
        Parameters
        ----------
        x : numpy array
            1 dimensional array that represents time series of data points
        n : int
            Highest scale to multiply to
        Returns
        -------
        prod : numpy array
            The multiscale product for x
        """
        n_pnts = x.size
        lambda_j = [1.5, 1.12, 1.03, 1.01][0:n]
        if n > 4:
            lambda_j += [1.0] * (n - 4)

        h = np.array([0.125, 0.375, 0.375, 0.125])
        g = np.array([2.0, -2.0])

        gn = [2]
        hn = [3]
        for j in range(1, n):
            q = 2 ** (j - 1)
            gn.append(q + 1)
            hn.append(3 * q + 1)

        s = x
        prod = np.ones(n_pnts)
        wavelets = np.ones((n, n_pnts))
        for j in range(n):
            s = np.concatenate((s[::-1], s, s[::-1]))
            n_zeros = 2 ** j - 1
            gz = self._insert_zeros(g, n_zeros)
            hz = self._insert_zeros(h, n_zeros)
            current = (1.0 / lambda_j[j]) * np.convolve(s, gz)
            current = current[n_pnts + gn[j] - 1:2 * n_pnts + gn[j] - 1]
            prod *= current
            wavelets[j] *= current
            s_new = np.convolve(s, hz)
            s = s_new[n_pnts + hn[j] - 1:2 * n_pnts + hn[j] - 1]
        prod /= np.abs(prod).max()
        return prod, wavelets

    @staticmethod
    def _insert_zeros(x, n):
        """
        From https://github.com/thomasbkahn/step-detect.

        Helper function for mz_fwt. Splits input array and adds n zeros
        between values.
        """
        newlen = (n + 1) * x.size
        out = np.zeros(newlen)
        indices = list(range(0, newlen - n, n + 1))
        out[indices] = x
        return out

    @staticmethod
    def find_steps(array, threshold=None, sd_threshold=2):
        """
        Based on the code at https://github.com/thomasbkahn/step-detect.

        Finds local maxima by segmenting array based on positions at which
        the threshold value is crossed.
        Parameters
        ----------
        array : numpy array
            1 dimensional array that represents time series of data points
        threshold : int / float
            Threshold value that defines a step. If no threshold value is specified, the sd_threshold parameter is used
            instead.
        sd_threshold : int
            Threshold defined as standard deviations of the data.
        Returns
        -------
        steps : list
            List of indices of the detected steps
        """
        if threshold is None:
            threshold = sd_threshold**2 * np.var(array)
        steps = []
        above_points = np.where(array > threshold, 1, 0)
        below_points = np.where(array < -threshold, 1, 0)
        ap_dif = np.diff(above_points)
        bp_dif = np.diff(below_points)
        pos_cross_ups = np.where(ap_dif == 1)[0]
        pos_cross_dns = np.where(ap_dif == -1)[0]
        neg_cross_dns = np.where(bp_dif == 1)[0]
        neg_cross_ups = np.where(bp_dif == -1)[0]
        # If cross_dns is longer that cross_ups, the first entry in cross_dns is zero, which will cause a crash
        if len(pos_cross_dns) > len(pos_cross_ups):
            pos_cross_dns = pos_cross_dns[1:]
        if len(neg_cross_ups) > len(neg_cross_dns):
            neg_cross_ups = neg_cross_ups[1:]
        for upi, dni in zip(pos_cross_ups, pos_cross_dns):
            steps.append(np.argmax(array[upi: dni]) + upi + 1)
        for dni, upi in zip(neg_cross_dns, neg_cross_ups):
            steps.append(np.argmin(array[dni: upi]) + dni + 1)
        return sorted(steps)

    @staticmethod
    def get_step_sizes(array, indices, window=1000):
        """
        A modified version of the code at https://github.com/thomasbkahn/step-detect.

        Calculates step size for each index within the supplied list. Step
        size is determined by averaging over a range of points (specified
        by the window parameter) before and after the index of step
        occurrence. The directionality of the step is reflected by the sign
        of the step size (i.e. a positive value indicates an upward step,
        and a negative value indicates a downward step). The combined
        standard deviation of both measurements (as a measure of uncertainty
        in step calculation) is also provided.
        Parameters
        ----------
        array : numpy array
            1 dimensional array that represents time series of data points
        indices : list
            List of indices of the detected steps (as provided by
            find_steps, for example)
        window : int, optional
            Number of points to average over to determine baseline levels
            before and after step.
        Returns
        -------
        step_sizes : list
            List of tuples describing the mean of the data before and after the detected step
        step_error : list
        """
        # TODO: Check the windows for estimating step size
        step_sizes = []
        step_error = []
        indices = sorted(indices)
        last = len(indices) - 1
        for i, index in enumerate(indices):
            if len(indices) == 1:
                q = min(window, index, len(array) - 1 - index)
            elif i == 0:
                q = min(window, indices[i + 1] - index, index)
            elif i == last:
                q = min(window, index - indices[i - 1], len(array) - 1 - index)
            else:
                q = min(window, index - indices[i - 1], indices[i + 1] - index)
            a = array[index - q: index + 1]
            b = array[index: index + q + 1]
            step_sizes.append((a.mean(), b.mean()))
            error = sqrt(a.var() + b.var())
            if isnan(error):
                step_error.append(abs(step_sizes[-1][1] - step_sizes[-1][0]))
            else:
                step_error.append(error)
        return step_sizes, step_error
