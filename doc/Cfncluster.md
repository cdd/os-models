
Technical notes to assist building clusters on AWS cloudformation using
cfncluster.  Used for hyperparamter tuning and large dataset (e.g. BAE) processing.

## Notes for using cfncluster

Docs at `http://cfncluster.readthedocs.io/en/latest/index.html`

Install on Anaconda `conda install cfncluster`.  Copy config from dist

```
cp /home/packages/anaconda3/envs/osmodels/lib/python3.6/site-packages/cfncluster/examples/config ~/.cfncluster/
```
Create grid engine and submit usin qsub:
```
qsub -V -b y -cwd /home/ec2-user/fourier 37 19
```

List jobs by node:
```
qconf -sh
```

Configure- don't set keys/security as use environment variables for this
```
cfncluster configure
```

#### Bae assay tips

Check for memory issues:

```
missing=`egrep -i 'error|exception' *.err| grep -i memory| awk 'BEGIN {FS=":"} {print $1}'|sort|uniq|sed -e 's/^assay_//' -e 's/\.err//'`
```

Resubmit ids
```
for f in $missing
do
rm assay_$f.out
rm assay_$f.err
cmd="qsub -V -N a_$f -cwd -pe smp 2 -o assay_$f.out -e assay_$f.err assay_$f.sh"
echo $cmd
eval $cmd
done
```

Rebuild csv file
```
java -cp /osmodels/target/os-models-1.0-SNAPSHOT-all.jar com.cdd.models.bae.BuildSummaryFile
```

Find sources of failure
```
grep -h '^java' */failure.txt|sort|uniq
```

Find missing results:
```
ids=`find . -name 'assay_*' -type d | sed -s 's/^\.\/assay_//'`

for f in $ids; do if [ ! -f "assay_$f/failure.txt" -a ! -f "assay_$f/regression_results.csv" ]; then echo $f; fi; done
```