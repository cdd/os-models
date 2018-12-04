import os, sys
import pandas as pd
from frozen import Frozen
from collections import namedtuple
from itertools import product
import math
from sklearn.metrics import mean_squared_error
import glob


class TuningSummary(Frozen):

    def __init__(self, classification=True, all_systems=False, subset=None):
        self.classification = classification
        self.all_systems = all_systems
        self.subset = subset
        if subset:
            assert subset in ['spark', 'regular']

        dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "summaries", self.subdir()))

        if classification:
            spark_results = ['rfc',
                             'lrc_fp', 'lsvc_descs',
                             'lrc_descs', 'lrc_fps', 'lrc_desc', 'nb_mn', 'lsvc_fp',
                             'lsvc_fps', 'lsvc_desc',
                             'dtc', 'nb_bn']
            user_results = ['smile_rfc_desc', 'svc_lin_fp', 'svc_tanimoto_fp', 'smile_nb', 'svc_lin_desc',
                            'smile_svc_rbf', 'smile_rfc_fp', 'svc_rbf_desc', 'nnc_fp',
                            'smile_svc_poly', 'nnc_desc', 'svc_rbf_fp', 'smile_svc_tanimoto_fp',
                            'weka_nb_mn', 'svc_poly_desc', 'weka_rfc',
                            'weka_fold_rfc', 'svc_poly_fp', 'weka_nb_bn']
        else:
            spark_results = [
                'glr_desc', 'gbt', 'glr_fps', 'lr_fps', 'lr_desc', 'lr_descs', 'lr_fp',
                'glr_fp', 'dtr', 'rfr'
            ]
            user_results = [
                'weka_rfr', 'smile_svr_tanimoto_fp', 'svr_rbf_desc', 'smile_rfr_desc', 'smile_svr_poly',
                'svr_tanimoto_fp',
                'weka_fold_rfr', 'svr_poly_fp', 'svr_poly_desc', 'svr_rbf_fp', 'smile_rfr_fp', 'smile_svr_rbf',
                'svr_lin_desc', 'svr_lin_fp', 'mltk_el_fold_fp', 'mltk_el', 'lr_el_desc', 'lr_el_fp', 'lr_el_fold_fp'
            ]

        if not subset:
            include_files = spark_results + user_results
            exclude_files = []
        elif subset == 'spark':
            include_files = spark_results
            exclude_files = user_results
        elif subset == 'regular':
            include_files = user_results
            exclude_files = spark_results
        else:
            raise ValueError()

        include_files = [os.path.join(dir, 'tune_{}_summary.csv'.format(f)) for f in include_files]
        exclude_files = [os.path.join(dir, 'tune_{}_summary.csv'.format(f)) for f in exclude_files]

        summary_files = glob.glob(dir + os.path.sep + "tune_*_summary.csv")
        for s in summary_files:
            print(s)
            assert s in include_files or s in exclude_files
            assert not (s in include_files and s in exclude_files)
        summary_files = [s for s in summary_files if s in include_files]

        methods = [os.path.basename(f)[5:-12] for f in summary_files]
        print("Loading results for {}".format(methods))
        summary_dfs = [pd.read_csv(f) for f in summary_files]

        if all_systems:
            no_systems = max([df['name'].nunique() for df in summary_dfs])
            print("Number of systems {}".format(no_systems))
            print("No of methods {}".format(len(summary_dfs)))
            summary_dfs, methods = zip(
                *[(df, m) for df, m in zip(summary_dfs, methods) if df['name'].nunique() == no_systems])
            print("No of methods for all systems {}".format(len(summary_dfs)))

        for method, df in zip(methods, summary_dfs):
            df['method'] = method
        summary_df = pd.concat(summary_dfs, ignore_index=True)

        self.summary_df = summary_df
        self.methods = methods
        self.summary_dfs = summary_dfs

    def subdir(self):
        if self.classification:
            return 'all_classification' if self.all_systems else 'classification'
        else:
            return 'regression'

    def metric_name(self):
        return 'auc' if self.classification else 'rmse'


if __name__ == "__main__":
    tuning_summary = TuningSummary(classification=False, subset=None)
