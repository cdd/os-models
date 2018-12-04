from __future__ import print_function

import glob
import os
import stat
import textwrap


class CreateValidationScripts:
    def __init__(self, validation_apps, classification=False, skip_large=False):
        self.classification = classification
        self.validation_apps = validation_apps
        self.skip_large = skip_large
        self.test_systems()

    def test_systems(self):
        test_files = glob.glob('{}/*.csv.gz'.format(self.test_dir()))
        systems = [os.path.basename(t)[:-7] for t in test_files]
        if self.skip_large:
            skip_systems = ['data_kcnq', 'data_bubonic']
            systems = [s for s in systems if s not in skip_systems]
        self.systems = systems
        print('Test systems are {}'.format(self.systems))

    def test_dir(self):
        return os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "data", "discrete" if self.classification else "continuous"))

    def create_job_files(self):
        jar_file = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "target", "os-models-1.0-SNAPSHOT-all.jar"))
        assert (os.path.exists(jar_file))
        for app, system in ((a, s) for a in self.validation_apps for s in self.systems):
            print("app {} sys {}".format(app, system))
            script = """\
            #!/bin/sh 
            java -Xmx3500M -Xms2048M -cp {} com.cdd.models.validation.{} {}
            """.format(jar_file, app, system)
            file = "{}_{}.sh".format(app, system)
            with open(file, 'w') as fh:
                fh.write(textwrap.dedent(script))
            st = os.stat(file)
            os.chmod(file, st.st_mode | stat.S_IEXEC)

    def create_submit_file(self, file_name):
        with open(file_name, 'w') as fh:
            for app, system in ((a, s) for a in self.validation_apps for s in self.systems):
                job = '{}_{}'.format(app, system)
                error = '{}.err'.format(job)
                out = '{}.out'.format(job)
                script = '{}.sh'.format(job)
                #  '-pe smp 4' for multicore reservation
                cmd = "qsub -V -N {} -cwd -pe smp 1 -o {} -e {} {}\n".format(job, out, error, script)
                fh.write(cmd)


def create_svm_regressor_scripts():
    creator = CreateValidationScripts(
        ['TuneSvmRegressorTanimoto', 'TuneSvmRegressorRbfFp', 'TuneSvmRegressorRbfDesc', 'TuneSvmRegressorPolynomialFp',
         'TuneSvmRegressorPolynomialDesc'])
    creator.create_job_files()
    creator.create_submit_file("submit_svm_regressors.sh")


def create_smile_svm_regressor_scripts():
    creator = CreateValidationScripts(
        ['TuneSmileSvmRegressorTanimoto', 'TuneSmileSvmRegressorRbf', 'TuneSmileSvmRegressorPolynomial'])
    creator.create_job_files()
    creator.create_submit_file("submit_smile_svm_regressors.sh")

def create_svm_classification_scripts():
    creator = CreateValidationScripts(
        ['TuneSvmClassifierTanimoto', 'TuneSvmClassifierRbfFp', 'TuneSvmClassifierRbfDesc',
         'TuneSvmClassifierPolynomialFp', 'TuneSvmClassifierPolynomialDesc'], classification=True, skip_large=True)
    creator.create_job_files()
    creator.create_submit_file("submit_svm_classifiers.sh")


def create_smile_svm_classification_scripts():
    creator = CreateValidationScripts(
        ['TuneSmileSvmClassifierTanimoto', 'TuneSmileSvmClassifierRbf', 'TuneSmileSvmClassifierPolynomial'],
        classification=True, skip_large=True)
    creator.create_job_files()
    creator.create_submit_file("submit_smile_svm_classifiers.sh")


def create_smile_linear_svm_classification_scripts():
    creator = CreateValidationScripts(
        ['TuneSmileClassifierLinear'],
        classification=True, skip_large=False)
    creator.create_job_files()
    creator.create_submit_file("submit_smile_linear_svm_classifiers.sh")


def create_liblinear_classification_scripts():
    creator = CreateValidationScripts(
        ['TuneLinearClassifierFp', 'TuneLinearClassifierDesc'], classification=True, skip_large=False)
    creator.create_job_files()
    creator.create_submit_file("submit_liblinear_classifiers.sh")


def create_liblinear_regression_scripts():
    creator = CreateValidationScripts(
        ['TuneLibLinearRegressorFp', 'TuneLinearRegressorDesc'])
    creator.create_job_files()
    creator.create_submit_file("submit_liblinear_regressors.sh")


def create_liblinear_smallfp_classification_scripts():
    creator = CreateValidationScripts(
        ['TuneLinearClassifierSmallFp'], classification=True, skip_large=False)
    creator.create_job_files()
    creator.create_submit_file("submit_smallfp_liblinear_classifiers.sh")


def create_epcoh_nn_classification_scripts():
    creator = CreateValidationScripts(
        ['TuneEpochNeuralNetworkClassifierFp', 'TuneEpochNeuralNetworkClassifierDesc'], classification=True,
        skip_large=True)
    creator.create_job_files()
    creator.create_submit_file("submit_epcoh_nn_classifiers.sh")


def create_naive_bayes_scripts():
    creator = CreateValidationScripts(
    ['TuneSmileNaiveBayes', 'TuneWekaNaiveBayesBernoulli', 'TuneWekaNaiveBayesMultiNominial'], classification=True,
    skip_large=False)
    creator.create_job_files()
    creator.create_submit_file('submit_naive_bayes_classifiers.sh')


def create_random_forest_regression_scripts():
    creator = CreateValidationScripts(
        ['TuneWekaRandomForestRegressor', 'TuneWekaRandomForestRegressorWithFolding',
         'TuneSmileRandomForestRegressorFp', 'TuneSmileRandomForestRegressorDesc'], classification=False)
    creator.create_job_files()
    creator.create_submit_file('submit_rf_regressors.sh')


def create_random_forest_classification_scripts():
    creator = CreateValidationScripts(
        ['TuneWekaRandomForestClassifier', 'TuneWekaRandomForestClassifierWithFolding',
         'TuneSmileRandomForestClassifierFp', 'TuneSmileRandomForestClassifierDesc'], classification=True, skip_large=True
    )
    creator.create_job_files()
    creator.create_submit_file('submit_rf_classifiers.sh')

def create_mltk_elastic_net_regression_scripts():
    creator = CreateValidationScripts(
        ['TuneMltkElasticNetRegressor', 'TuneMltkElasticNetRegressorFoldedFp'], classification=False)
    creator.create_job_files()
    creator.create_submit_file('submit_mktk_el_regressors.sh')


def create_elastic_net_regression_scripts():
    creator = CreateValidationScripts(
        ['TuneLinearRegressorsDescriptors', 'TuneLinearRegressorsSparseFp', 'TuneLinearRegressorsFoldedFp'], classification=False)
    creator.create_job_files()
    creator.create_submit_file('submit_mktk_el_regressors.sh')


if __name__ == "__main__":
    # create_svm_regressor_scripts()
    # create_svm_classification_scripts()
    #create_liblinear_classification_scripts()
    # create_liblinear_regression_scripts()
    # create_liblinear_smallfp_classification_scripts()
    # create_epcoh_nn_classification_scripts()
    #create_naive_bayes_scripts()
    #create_random_forest_classification_scripts()
    #create_random_forest_regression_scripts()
    #create_smile_svm_classification_scripts()
    #create_smile_svm_regressor_scripts()
    #create_smile_linear_svm_classification_scripts()
    #create_mltk_elastic_net_Regression_scripts()
    create_elastic_net_regression_scripts()