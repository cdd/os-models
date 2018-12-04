## Vault Public Data

Notes for processing Vault public data and the derivation of the Universal Metric.

#### ML classifier for readout fields.

See `data/vault/readout_fields_training.csv` this is
`data/vault/readout_fields.csv` with the addition of a use_readout
column.  Readout_fields.csv is created by app ProcessDatasetInformation() in VaultReadouts.

Model for field identification is created in `data/vault/readout_fields_model.obj`
by class LabelIdentifier.  Model is used when creating vault data tables from
a dataset/protocol data table in method datasetInformationAndDataTables, trait ProtocolFilesFinders (real work is done in method toDataTables class ProtocolFeatureReadouts).

#### Serialized File Information


In VaultReadouts.scala:

Dataset information is cached in data/vault/datasetInformation.obj (will be rebuilt if deleted).

Objects DownloadPublicProtocols will download data to data/vault/protocol_molecule_downloads
and AddMolecularFeaturesToProtocol will add features to data/vault/protocol_molecule_features
Will be rebuilt if deleted

Both object files have the format `s"v_${vaultId}_d_${dataset.name}_p_${protocol.name}.obj".replace("/", "")`

serialized vault data tables will be stored in directory `s"v_${vaultId}_p_${protocol.id}_${protocol.name}".replace('/', '_')`

To download a dataset and protocol again:

```
java -cp target/os-models-1.0-SNAPSHOT-all.jar com.cdd.models.vault.DownloadPublicProtocols --dataset 2420 --protocol 2166 --remove
java -cp target/os-models-1.0-SNAPSHOT-all.jar com.cdd.models.vault.AddMolecularFeaturesToProtocols  --dataset 2420 --protocol 2166 --remove
```

To download everything remove object files by hand and:
```
java -cp target/os-models-1.0-SNAPSHOT-all.jar com.cdd.models.vault.DownloadPublicProtocols
java -cp target/os-models-1.0-SNAPSHOT-all.jar com.cdd.models.vault.AddMolecularFeaturesToProtocols
```

To list all downloaded protocols use app `com.cdd.models.vault.ListDownloadedProtocolsApp`

Machine learning models are also stored in the vault data table directories.

#### Generation of regression models and single point classification models

App com.cdd.models.vault.PublicVaultEstimatorApp runs regression models and single point classifiers on the test systems (defined by protocol objects
`"*PDSP*.obj", "*SARfari*.obj", "*PKIS*.obj"`)

```
java -cp target/os-models-1.0-SNAPSHOT-all.jar com.cdd.models.vault.PublicVaultEstimatorApp
```

This app outputs summary output (performance of best regression and classification models in each system) in

```
data/vault/protocol_molecule_features/current_regression.csv
data/vault/protocol_molecule_features/current_classification.csv
```

Performance of all regression and classification models are detailed in

```
data/vault/protocol_molecule_features/current_regression_detail.csv
data/vault/protocol_molecule_features/current_classification_detail.csv
```

This will create serialized data tables and regression and classification models in the
vault data table directories:

- `vaultDataTables.obj` contains a list of data table like objects with
 each item containing a data table with a label column derived from
 protocol readout fields (see ML classifier for readout fields above)
- `<VID>_regression.obj` for regression results
- `<VID>_classification_FixedPercentage_33.0.obj` and related for
fixed classification results

#### Generation of classification models from regression models

App com.cdd.models.vault.PublicVaultRegressionToMultipleClassificationCutoff
creates `data/vault/protocol_molecule_features/regression_to_classifier_cutoffs.csv`,
 by running classification from regression over the test systems
 (defined by protocol objects `"*PDSP*.obj", "*SARfari*.obj", "*PKIS*.obj"`).

This creates classification models from the saved regression models.
The input activity cutoff is chosen to maximize rocAUC (at least 10
 compounds and 5% of compounds must be active) and the output probability
activity threshold is chosen to maximize f1.
An additional filter is added such that f1 is at least 0.5.
The CSV file contains best results per test system.

Detailed results per regression algorithm per input label threshold are in
`data/vault/protocol_molecule_features/regression_to_classifier_cutoffs_detail.csv`.



#### Creation of CSV for for Universal Metric

The GUI app com.cdd.models.vault.AnalyzeClassificationAndRegression
reads  `data/vault/protocol_molecule_features/regression_to_classifier_cutoffs.csv` and
`data/vault/protocol_molecule_features/current_regression_detail.csv`.
Displays best regression and classification information and allows manual category assignment.
Manual categories are saved in serialized file
`data/vault/protocol_molecule_features/info_and_categories.obj`
This contains a map keyed by test system information with
categories (regression/Classification/both/bad) as values.

Use the `Save CSV` button to create the input CSV
`data/vault/protocol_molecule_features/regression_classification_categories_summary.csv`
for training the classifier
for whether or not a test system is best described by classification or regression.
The CSV will take along time to create.

#### ML for choosing bad/classification or regression for a given dataset

For Universal metric purposes we merge the both and regression categories.
Bad datasets are identified by rocAuc < .6.  The classifier is evaluated
using app `com.cdd.models.vault.ClassificationAndRegressionMLApp`.
The 3 fold cross validation for the available classifiers is written to `data/vault/protocol_molecule_features/opt_classification_and_regression_ml.csv`.

There is also a voting classifier- run app
 `com.cdd.models.vault.ClassificationAndRegressionVotingMLApp`.  The
 summary is written to stdout.

Tuning is done on a manual basis: run app
`com.cdd.models.vault.TuneClassificationAndRegressionML`.  A summary of
 3 fold cross validation values is written out to summary files in directory `data/vault/protocol_molecule_features/classification_and_regression_ml_tuning`.
 Copy the tuning files to `jupyter-notebooks/vault`:

```
 cd ~/src/os-models/data/vault/protocol_molecule_features/classification_and_regression_ml_tuning
 cp *.csv ~/src/os-models/jupyter-notebooks/vault/
```

Run notebook
 `Classification and Regression ML Tuning` to determine optimal
 parameters and set the optimal estimators' parameters in object
 `com.cdd.models.vault.ClassificationAndRegressionML`
 method `createOptimizedEstimators` appropriately.

Application `com.cdd.models.vault.BuildAndSaveModelsOnAllData` creates
and saves models on all data (i.e. no cross validation).  Includes the
voting classifier.  Saved models are written in directory `data/vault/protocol_molecule_features/classification_regression_ml_models/`

#### Running Universal Metric on Unseen Data

Class `com.cdd.models.vault.ClassificationAndRegressionMLValidate`
performs Universal Metric workflow on previously unseen datasets.

These may be visualized using application
`com.cdd.models.vault.AnalyzeClassificationAndRegressionMLValidateApp`.

#### TODO

* Replace jupyter notebooks with scala code
* Refactor categories map to have additional tracking information
* Redo ML to use best regression algorithm
* Perform ML search on eMolecules or similar



