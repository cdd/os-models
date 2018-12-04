
import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.RegressionEvaluator
import com.cdd.models.pipeline.estimator.SmileRidgeRegressor
import com.cdd.models.pipeline.transformer.{FilterSameFeatureValues, VectorFolder}
import com.cdd.models.pipeline.tuning.DataSplit
import org.scalatest.{FunSpec, Matchers}

class SmileRidgeRegressorSpec  extends FunSpec with Matchers{
  describe("Running Smile Random Forest regression model on RDKIT fingerprints") {

    //var dt = DataTable.loadProjectFile("data/continuous/Chagas.csv.gz")
    var dt = DataTable.loadProjectFile("data/continuous/MalariaGSK.csv.gz")

    dt = dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("fingerprints_RDKit_FCFP6" -> "features")).filterNull().shuffle(seed=4321L)

    it("should have loaded 13403 molecules") {
      dt.length should be(13403)
    }

    val filter = new FilterSameFeatureValues()
    dt = filter.fit(dt).transform(dt)
    val folder = new VectorFolder().setFoldSize(1000).setFeaturesColumn("uniqueFeatures")
    dt = folder.transform(dt)
    val (testDt, trainDt) = dt.testTrainSplit()

    val smileRidge = new SmileRidgeRegressor()
      .setFeaturesColumn("foldedFeatures")
      .setLambda(0.3)
    val rfrModel = smileRidge.fit(trainDt)

    val predictDt = rfrModel.transform(testDt)

    val evaluator = new RegressionEvaluator()
    val rmse = evaluator.evaluate(predictDt)

    val c = evaluator.correlation(predictDt)

    it("should have a test model rmse of about 0.33") {
      rmse should be(0.33 +- 0.1)
    }

    it("should have a test model correlation of about 0.57") {
      c should be(0.57 +- 0.1)
    }

    val dataSplit = new DataSplit().setEvaluator(evaluator).setEstimator(smileRidge)
    dataSplit.foldAndFit(dt)

    info(s"Metric is ${dataSplit.getMetric()}")
    val c2 = evaluator.correlation(dataSplit.getPredictDt())
    info(s"Correlation is ${c2}")

    it("should have a datasplit model rmse of about 0.33") {
      dataSplit.getMetric() should be(0.33 +- 0.1)
    }

    it("should have a datasplit model correlation of about 0.59") {
      c2 should be(0.59 +- 0.1)
    }

  }

}
