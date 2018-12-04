import os, sys, re
import pandas as pd
from frozen import Frozen
from collections import namedtuple
from itertools import product
import math
from sklearn.metrics import mean_squared_error
import glob


class TuningResults(Frozen):

    def __init__(self, csv_file, classification=True, all_systems=False):
        self.csv_file = csv_file
        self.classification = classification
        self.all_systems = all_systems
        file = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "results", self.subdir(), csv_file))
        self.results = pd.read_csv(file)
        self.rename_columns()
        self.ok = self.check()
        if not self.ok:
            return
        self.parameters = [c for c in self.results.columns if c not in ['name', 'method', 'f1', 'f2', 'f3', 'rmse', 'auc']]
        print ('Found parameters {}'.format(self.parameters))
        self.global_summary_df = None
        self.best_summary_df = None
        self.best_parameters = None
        self.global_common_rmse = None
        self.summary_df = None
        self.category_df = None

    def rename_columns(self):
        def rename_col(no, n):
            new_n = re.sub(r" \[.*\]$", "", n)
            if new_n == 'NAME':
                return 'name'
            elif new_n == 'METHOD':
                return 'method'
            elif new_n.startswith("rmse_"):
                return re.sub(r"^rmse_", "f", new_n)
            elif new_n.startswith("auc_"):
                return re.sub(r"auc_", "f", new_n)
            elif new_n == 'rocAuc':
                return 'auc'
            elif new_n.startswith("rocAuc_"):
                return re.sub(r"rocAuc_", "f", new_n)
            elif new_n == 'lambda':
                return 'C'
            elif new_n == 'method' and no >= 2:
                return 'submethod'
            else:
                return new_n

        columns_map = {n:rename_col(i, n) for i, n in enumerate(self.results.columns)}
        self.results.rename(columns=columns_map, inplace=True)

    def check(self):
        if self.classification:
            expected_count = 8 if self.all_systems else 6
        else:
            expected_count = 10

        if self.classification and not self.all_systems:
            self.results = self.results[~self.results['name'].isin(['data_bubonic', 'data_kcnq'])]

        count = len(self.results['name'].unique())
        if count != expected_count:
            print('Number of test systems not correct: expected {} got {}: skipping {}'
                  .format(expected_count, count, self.csv_file))
            return False

        return True

    def summary_subdir(self):
        if self.classification:
            return 'all_classification' if self.all_systems else 'classification'
        else:
            return 'regression'

    def subdir(self):
        return 'classification' if self.classification else 'regression'

    def print_parameter_values(self):
        for parameter in self.parameters:
            print("{} {}".format(parameter, list(self.results[parameter].value_counts().to_dict().keys())))

    def metric_name(self):
        return 'auc' if self.classification else 'rmse'

    def global_summary(self):
        if self.classification:
            global_summary_df = self.results.loc[
                self.results.groupby("name", as_index=False)[self.metric_name()].idxmax()].sort_values('name')
        else:
            global_summary_df = self.results.loc[
                self.results.groupby("name", as_index=False)[self.metric_name()].idxmin()].sort_values('name')
        self.global_summary_df = global_summary_df
        return global_summary_df

    def best_summary(self):
        params = self._param_order(self.parameters)
        Summary = namedtuple('Summary', params + ['rmse'])
        param_values = [list(self.results[p].value_counts().to_dict().keys()) for p in params]
        print("Iterating over:")
        for p, values in zip(params, param_values):
            str_vals = [self._format_param_value(v) for v in sorted(values)]
            print("{} ({})".format(self._format_param(p), ', '.join(str_vals)))
        combinations = product(*param_values)
        best = None
        global_col = '{}_global'.format(self.metric_name())
        test_col = '{}_test'.format(self.metric_name())
        for values in combinations:
            query = '&'.join([" ({} == {}) ".format(p, "'{}'".format(v) if isinstance(v, str) else v) for p, v in
                              zip(params, values)])
            test_df = self.results.query(query)
            join_df = self.global_summary_df.merge(test_df, how='inner', on='name', suffixes=('_global', '_test'))[
                ['name', global_col, test_col]]
            rmse = math.sqrt(mean_squared_error(join_df[global_col], join_df[test_col]))
            if best is None or best.rmse > rmse:
                summary = list(values) + [rmse]
                best = Summary(*summary)

        print("Best common settings:")
        for f in params:
            v = getattr(best, f)
            print("{}: {}".format(self._format_param(f), self._format_param_value(v)))
        print("RMSE: {}".format(best.rmse))

        query = '&'.join(
            [" ({} == {}) ".format(p, "'{}'".format(v) if isinstance(v, str) else v) for p, v in zip(params, best)])
        best_summary_df = self.results.query(query)[['name', self.metric_name()]].sort_values('name')

        self.best_summary_df = best_summary_df
        self.best_parameters = best
        return best_summary_df

    def _param_order(self, params):
        params = list(params)
        if 'featuresColumn' in params:
            feature_name = 'featuresColumn'
        elif 'feature' in params:
            feature_name = 'feature'
        else:
            assert False
        params.remove(feature_name)
        params.sort(key=lambda p: self._format_param(p))

        return [feature_name]+params

    def _format_param(self, param):
        if param == 'feature':
            return 'Features'
        if param == 'featuresColumn':
            return 'Features'
        if param == 'offset':
            return 'Coef0'
        if param == 'maxDepth':
            return 'Max tree depth'
        if param == 'numTrees':
            return 'Max trees'
        if param == 'maxIter':
            return 'Max iterations'
        if param == 'maxNodes':
            return 'Max tree nodes'
        #if param == 'C':
        #    return 'Regularisation parameter'
        if param == 'reg':
            return 'Regularization parameter'
        if param == 'scalerType':
            return 'Scaler'
        if param == 'foldSize':
            return 'Fold size'
        if param == 'regularizationType':
            return 'Regularization'
        if param == 'submethod':
            return 'Method'
        if param == 'C':
            return 'Regularization parameter'
        if param == 'alpha':
            return 'Bayes Laplacian smoothing'

        return "{}{}".format(param[:1].upper(), param[1:])

    def _format_param_value(self, value):
        if value == 'rdkit_fp':
            return 'rdkit_fcfp'
        if value == 'cdk_fp':
            return 'cdk_fcfp'
        if value == 'MaxMin':
            return 'Range'

        return str(value)

    def best_summary_by_feature(self):
        params = self.parameters
        assert 'feature' in params or 'featuresColumn' in params
        feature_name = 'featuresColumn' if 'featuresColumn' in params else 'feature'
        params.remove(feature_name)
        features = list(self.results[feature_name].value_counts().to_dict().keys())
        Summary = namedtuple('Summary', params + ['test_rmse', 'rmse'])
        param_values = [list(self.results[p].value_counts().to_dict().keys()) for p in params]
        print('Features: {}'.format(features))
        print("Iterating over:")
        for p, values in zip(params, param_values):
            print("{}: {}".format(p, values))
        best_by_feature = {}

        global_col = '{}_global'.format(self.metric_name())
        test_col= '{}_test'.format(self.metric_name())

        for feature in features:
            for values in product(*param_values):
                query = '&'.join([" ({} == {}) ".format(p, "'{}'".format(v) if isinstance(v, str) else v) for p, v in
                                  zip(params, values)])
                query = "{} & ({} == '{}')".format(query, feature_name, feature)
                test_df = self.results.query(query)
                join_df = self.global_summary_df.merge(test_df, how='inner', on='name', suffixes=('_global', '_test'))[
                    ['name', global_col, test_col]]
                rmse = math.sqrt(mean_squared_error(join_df[global_col], join_df[test_col]))
                avg = join_df[test_col].mean()
                best = best_by_feature.get(feature)
                if best is None or best.rmse > rmse:
                    summary = list(values) + [avg, rmse]
                    best = Summary(*summary)
                    best_by_feature[feature] = best

        if False:
            print("Best common settings by feature:")
            for feature in features:
                print('Feature : {}'.format(feature))
                for f, v in best_by_feature[feature]._asdict().items():
                    print("{} -> {}".format(f, v))

        data = [best_by_feature[k] for k in features]
        df = pd.DataFrame(data, columns=Summary._fields)
        df['feature'] = features
        df = df.sort_values('rmse')
        return df

    def summary(self):
        global_col = '{}_global'.format(self.metric_name())
        common_col= '{}_common'.format(self.metric_name())
        summary_df = self.global_summary_df.merge(self.best_summary_df, how='inner', on='name', suffixes=('_global', '_common'))[['name', global_col, common_col]]
        summary_df['delta'] = summary_df[common_col] - summary_df[global_col]
        if self.classification:
            summary_df['delta'] = -summary_df['delta']
        file = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "summaries", self.summary_subdir(), self.csv_file[:-4]+"_summary.csv"))
        print('summary file is {}'.format(file))
        summary_df.to_csv(file, index=False)
        self.summary_df = summary_df
        print('Mean common metric {:.3f} global metric {:.3f}'.format(summary_df[common_col].mean(), summary_df[global_col].mean()))
        rmse = math.sqrt(mean_squared_error(summary_df[global_col], summary_df[common_col]))
        print('global to common RMSE is {:.3f}'.format(rmse))
        self.global_common_rmse = rmse
        return summary_df

    def category(self):
        self.global_summary_df['cat']='global'
        self.best_summary_df['cat']='common'
        category_df = self.best_summary_df.append(self.global_summary_df)[['name', 'cat', self.metric_name()]]
        self.category_df = category_df
        return category_df


def process_file(csv_file, classification, all_systems):
    csv_file = os.path.basename(csv_file)
    print("\n## Processing file {} ###\n".format(csv_file))
    tuning_results = TuningResults(csv_file, classification=classification, all_systems=all_systems)
    if tuning_results.ok:
        print("Results shape {}".format(tuning_results.results.shape))
        tuning_results.print_parameter_values()
        print('Min summary shape {}'.format(tuning_results.global_summary().shape))
        print('Best summary shape {}'.format(tuning_results.best_summary().shape))
        print('Summary shape {}'.format(tuning_results.summary().shape))
        print('Category shape {}'.format(tuning_results.category().shape))

        print("\nBest Summary:")
        tuning_results.best_summary()

        print("\nSummary:")
        tuning_results.summary()


def show_information(csv_file, classification, all_systems):
    csv_file = os.path.basename(csv_file)
    print("\n## Processing file {} ###\n".format(csv_file))
    tuning_results = TuningResults(csv_file, classification=classification, all_systems=all_systems)
    if tuning_results.ok:
        tuning_results.global_summary()
        tuning_results.best_summary()
        tuning_results.summary()


def process_files(classification, all_systems):
    dir = 'classification' if classification else 'regression'
    dir = '../results/{}/*.csv'.format(dir)
    files = glob.glob(dir)
    for f in files:
        process_file(f, classification, all_systems)


if __name__ == "__main__":
    args = sys.argv[1:]
    if args:
        assert(len(args) >= 3)
        classification = args[0].lower() == 'true'
        all_systems = args[1].lower() == 'true'
        files = args[2:]
        for file in files:
            #process_file(file, classification, all_systems)
            show_information(file, classification, all_systems)
    else:
        #process_files(classification=True, all_systems=True)
        #process_files(classification=True, all_systems=False)
        process_files(classification=False, all_systems=True)

