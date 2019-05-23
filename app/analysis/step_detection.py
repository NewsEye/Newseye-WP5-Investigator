import pandas as pd
import numpy as np
from math import sqrt, isnan
from app import db

from app.analysis.analysis_utils import AnalysisUtility

class FindStepsFromTimeSeries(AnalysisUtility):
    def __init__(self):
        super(FindStepsFromTimeSeries, self).__init__()
        self.utility_name = 'find_steps_from_time_series'
        self.utility_description = ''
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

    async def __call__(self, task, use_data=False):
        if not use_data:
            column_name = task.task_parameters.get('column_name')
            step_threshold = task.task_parameters.get('step_threshold')

            # looks for tasks to be done before this one
            # TODO: avoid it, will be done by Planner
            input_task = self.get_input_task(task)
            task.hist_parent_id = input_task.uuid
            db.session.commit()
            if input_task is None or input_task.task_status != 'finished':
                raise TypeError("No task results available for analysis")
            input_data = input_task.task_result.result

            input_data = pd.DataFrame(input_data['relative_counts'])
            input_data.index = pd.to_numeric(input_data.index)
            index_start = input_data.index.min()
            index_end = input_data.index.max()
            idx = np.arange(index_start, index_end + 1)
            input_data = input_data.reindex(idx, fill_value=0)
        else:
            column_name = None
            input_data = task
            step_threshold = None
        steps = {}
        if column_name:
            columns = [column_name]
        else:
            columns = input_data.columns
        for column in columns:
            data = input_data[column]
            prod, _ = self.mz_fwt(data, 3)
            step_indices = self.find_steps(prod, step_threshold)
            step_sizes, errors = self.get_step_sizes(input_data[column], step_indices)
            step_times = [int(input_data.index[idx]) for idx in step_indices]
            steps[column] = list(zip(step_times, step_sizes, errors))
        # TODO: Fix output to match documentation
        # TODO: Implement interestingness values
        return steps

    def mz_fwt(self, x, n=3):
        """
        A modified version of the code at https://github.com/thomasbkahn/step-detect:

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
    def find_steps(array, threshold=None):
        """
        A modified version of the code at https://github.com/thomasbkahn/step-detect.

        Finds local maxima by segmenting array based on positions at which
        the threshold value is crossed. Note that this thresholding is
        applied after the absolute value of the array is taken. Thus,
        the distinction between upward and downward steps is lost. However,
        get_step_sizes can be used to determine directionality after the
        fact.
        Parameters
        ----------
        array : numpy array
            1 dimensional array that represents time series of data points
        threshold : int / float
            Threshold value that defines a step. If no threshold value is specified, it is set to 2 standard deviations
        Returns
        -------
        steps : list
            List of indices of the detected steps
        """
        if threshold is None:
            threshold = 4 * np.var(array)  # Use 2 standard deviations as the threshold
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

    # TODO: instead of using just the original data, perhaps by odd-symmetric periodical extension??
    #  This should improve the accuracy close to the beginning and end of the signal
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
