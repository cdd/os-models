from __future__ import print_function

import glob
import os
import stat
import textwrap


class CreateTuningScripts:
    tox_fields = ["NR-AR", "NR-AhR", "NR-AR-LBD", "NR-ER", "NR-ER-LBD", "NR-Aromatase", "NR-PPAR-gamma",
                  "SR-ARE", "SR-ATAD5", "SR-HSE", "SR-MMP", "SR-p53"]
    tuners = [
        'OptimizeWekaRfClassification',
        'OptimizeSmileSvmClassifierTanimoto',
        'OptimizeSmileSvmClassifierPoly',
        'OptimizeSmileSvmClassifierRbf',
        'OptimizeWekaRfClassificationFoldedFp',
        'OptimizeSmileNaiveBayesClassifier'
    ]

    def __init__(self):
        pass

    def create_job_files(self):
        jar_file = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "target", "os-models-1.0-SNAPSHOT-all.jar"))
        assert (os.path.exists(jar_file))
        for tuner, tox_field in ((t, f) for t in self.tuners for f in self.tox_fields):
            print("tuner {} field {}".format(tuner, tox_field))
            script = """\
            #!/bin/sh 
            java -Xmx3500M -Xms2048M -cp {} com.cdd.models.tox.tuning.{} {}
            """.format(jar_file, tuner, tox_field)
            file = "{}_{}.sh".format(tuner, tox_field)
            with open(file, 'w') as fh:
                fh.write(textwrap.dedent(script))
            st = os.stat(file)
            os.chmod(file, st.st_mode | stat.S_IEXEC)

    def create_submit_file(self, file_name):
        with open(file_name, 'w') as fh:
            for tuner, tox_field in ((t, f) for t in self.tuners for f in self.tox_fields):
                job = '{}_{}'.format(tuner, tox_field)
                error = '{}.err'.format(job)
                out = '{}.out'.format(job)
                script = '{}.sh'.format(job)
                #  '-pe smp 4' for multicore reservation
                cmd = "qsub -V -N {} -cwd -pe smp 1 -o {} -e {} {}\n".format(job, out, error, script)
                fh.write(cmd)


if __name__ == "__main__":
    creator = CreateTuningScripts()
    creator.create_job_files()
    creator.create_submit_file('submit_tuners.sh')
