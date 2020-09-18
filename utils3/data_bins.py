#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
@File    : data_bins.py
@Time    : 2020-09-18 13:50
@Author  : yantingting
@Email   : yanxt123456@163.com
@Software: PyCharm
"""

import pandas as pd
from utils3.data_io_utils import print_run_time

class DataBins():
    def __init__(self):
        pass

    @print_run_time
    def woebin(self, dt, y, x=None,
               var_skip=None, breaks_list=None, special_values=None,
               stop_limit=  .1, count_distr_limit=0.05, bin_num_limit=8,
               positive="bad|1", no_cores=None, print_step=0, method="tree",
               save_breaks_list=None, **kwargs):
        '''
        WOE Binning
        ------
        `woebin` generates optimal binning for numerical, factor and categorical
        variables using methods including tree-like segmentation or chi-square
        merge. woebin can also customizing breakpoints if the breaks_list or
        special_values was provided.

        The default woe is defined as ln(Distr_Bad_i/Distr_Good_i). If you
        prefer ln(Distr_Good_i/Distr_Bad_i), please set the argument `positive`
        as negative value, such as '0' or 'good'. If there is a zero frequency
        class when calculating woe, the zero will replaced by 0.99 to make the
        woe calculable.

        Params
        ------
        dt: A data frame with both x (predictor/feature) and y (response/label) variables.
        y: Name of y variable.
        x: Name of x variables. Default is None. If x is None,
          then all variables except y are counted as x variables.
        var_skip: Name of variables that will skip for binning. Defaults to None.
        breaks_list: List of break points, default is None.
          If it is not None, variable binning will based on the
          provided breaks.
        special_values: the values specified in special_values
          will be in separate bins. Default is None.
        count_distr_limit: The minimum percentage of final binning
          class number over total. Accepted range: 0.01-0.2; default
          is 0.05.
        stop_limit: Stop binning segmentation when information value
          gain ratio less than the stop_limit, or stop binning merge
          when the minimum of chi-square less than 'qchisq(1-stoplimit, 1)'.
          Accepted range: 0-0.5; default is 0.1.
        bin_num_limit: Integer. The maximum number of binning.
        positive: Value of positive class, default "bad|1".
        no_cores: Number of CPU cores for parallel computation.
          Defaults None. If no_cores is None, the no_cores will
          set as 1 if length of x variables less than 10, and will
          set as the number of all CPU cores if the length of x variables
          greater than or equal to 10.
        print_step: A non-negative integer. Default is 1. If print_step>0,
          print variable names by each print_step-th iteration.
          If print_step=0 or no_cores>1, no message is print.
        method: Optimal binning method, it should be "tree" or "chimerge".
          Default is "tree".
        ignore_const_cols: Logical. Ignore constant columns. Defaults to True.
        ignore_datetime_cols: Logical. Ignore datetime columns. Defaults to True.
        check_cate_num: Logical. Check whether the number of unique values in
          categorical columns larger than 50. It might make the binning process slow
          if there are too many unique categories. Defaults to True.
        replace_blank: Logical. Replace blank values with None. Defaults to True.
        save_breaks_list: The file name to save breaks_list. Default is None.

        Returns
        ------
        dictionary
            Optimal or customized binning dataframe.

        Examples
        ------
        import scorecardpy as sc
        import pandas as pd

        # load data
        dat = sc.germancredit()

        # Example I
        # binning of two variables in germancredit dataset
        bins_2var = sc.woebin(dat, y = "creditability",
          x = ["credit.amount", "purpose"])

        # Example II
        # binning of the germancredit dataset
        bins_germ = sc.woebin(dat, y = "creditability")

        # Example III
        # customizing the breakpoints of binning
        dat2 = pd.DataFrame({'creditability':['good','bad']}).sample(50, replace=True)
        dat_nan = pd.concat([dat, dat2], ignore_index=True)

        breaks_list = {
          'age.in.years': [26, 35, 37, "Inf%,%missing"],
          'housing': ["own", "for free%,%rent"]
        }
        special_values = {
          'credit.amount': [2600, 9960, "6850%,%missing"],
          'purpose': ["education", "others%,%missing"]
        }

        bins_cus_brk = sc.woebin(dat_nan, y="creditability",
          x=["age.in.years","credit.amount","housing","purpose"],
          breaks_list=breaks_list, special_values=special_values)
        '''
        # arguments
        ## print_info
        print_info = kwargs.get('print_info', True)
        ## init_count_distr
        min_perc_fine_bin = kwargs.get('min_perc_fine_bin', None)
        init_count_distr = kwargs.get('init_count_distr', min_perc_fine_bin)
        if init_count_distr is None: init_count_distr = 0.02
        ## count_distr_limit
        min_perc_coarse_bin = kwargs.get('min_perc_coarse_bin', None)
        if min_perc_coarse_bin is not None: count_distr_limit = min_perc_coarse_bin
        ## bin_num_limit
        max_num_bin = kwargs.get('max_num_bin', None)
        if max_num_bin is not None: bin_num_limit = max_num_bin

        # print infomation
        if print_info:
            print('[INFO] creating woe binning ...')

        xs_len = len(xs)
        # print_step
        print_step = check_print_step(print_step)
        # breaks_list
        breaks_list = check_breaks_list(breaks_list, xs)
        # special_values
        special_values = check_special_values(special_values, xs)
        ### ###
        # stop_limit range
        if stop_limit < 0 or stop_limit > 0.5 or not isinstance(stop_limit, (float, int)):
            warnings.warn(
                "Incorrect parameter specification; accepted stop_limit parameter range is 0-0.5. Parameter was set to default (0.1).")
            stop_limit = 0.1
        # init_count_distr range
        if init_count_distr < 0.01 or init_count_distr > 0.2 or not isinstance(init_count_distr, (float, int)):
            warnings.warn(
                "Incorrect parameter specification; accepted init_count_distr parameter range is 0.01-0.2. Parameter was set to default (0.02).")
            init_count_distr = 0.02
        # count_distr_limit
        if count_distr_limit < 0.01 or count_distr_limit > 0.2 or not isinstance(count_distr_limit, (float, int)):
            warnings.warn(
                "Incorrect parameter specification; accepted count_distr_limit parameter range is 0.01-0.2. Parameter was set to default (0.05).")
            count_distr_limit = 0.05
        # bin_num_limit
        if not isinstance(bin_num_limit, (float, int)):
            warnings.warn("Incorrect inputs; bin_num_limit should be numeric variable. Parameter was set to default (8).")
            bin_num_limit = 8
        # method
        if method not in ["tree", "chimerge"]:
            warnings.warn("Incorrect inputs; method should be tree or chimerge. Parameter was set to default (tree).")
            method = "tree"
        ### ###
        # binning for each x variable
        # loop on xs
        if (no_cores is None) or (no_cores < 1):
            all_cores = mp.cpu_count() - 1
            no_cores = int(np.ceil(xs_len / 5 if xs_len / 5 < all_cores else all_cores * 0.9))
        if platform.system() == 'Windows':
            no_cores = 1

        # ylist to str
        y = y[0]
        # binning for variables
        if no_cores == 1:
            # create empty bins dict
            bins = {}
            for i in np.arange(xs_len):
                x_i = xs[i]
                # print(x_i)
                # print xs
                if print_step > 0 and bool((i + 1) % print_step):
                    print(('{:' + str(len(str(xs_len))) + '.0f}/{} {}').format(i, xs_len, x_i), flush=True)
                # woebining on one variable
                bins[x_i] = woebin2(
                    dtm=pd.DataFrame({'y': dt[y], 'variable': x_i, 'value': dt[x_i]}),
                    breaks=breaks_list[x_i] if (breaks_list is not None) and (x_i in breaks_list.keys()) else None,
                    spl_val=special_values[x_i] if (special_values is not None) and (
                                x_i in special_values.keys()) else None,
                    init_count_distr=init_count_distr,
                    count_distr_limit=count_distr_limit,
                    stop_limit=stop_limit,
                    bin_num_limit=bin_num_limit,
                    method=method
                )

        else:
            pool = mp.Pool(processes=no_cores)
            # arguments
            args = zip(
                [pd.DataFrame({'y': dt[y], 'variable': x_i, 'value': dt[x_i]}) for x_i in xs],
                [breaks_list[i] if (breaks_list is not None) and (i in list(breaks_list.keys())) else None for i in xs],
                [special_values[i] if (special_values is not None) and (i in list(special_values.keys())) else None for i in
                 xs],
                [init_count_distr] * xs_len, [count_distr_limit] * xs_len,
                [stop_limit] * xs_len, [bin_num_limit] * xs_len, [method] * xs_len
            )
            # bins in dictionary
            bins = dict(zip(xs, pool.starmap(woebin2, args)))
            pool.close()

        if save_breaks_list is not None:
            bins_to_breaks(bins, dt, to_string=True, save_string=save_breaks_list)
        # return
        return bins



