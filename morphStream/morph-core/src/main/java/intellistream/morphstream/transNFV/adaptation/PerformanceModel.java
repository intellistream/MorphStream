package intellistream.morphstream.transNFV.adaptation;

import org.jpmml.evaluator.*;
import org.jpmml.model.PMMLUtil;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.Model;
import org.dmg.pmml.PMML;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PerformanceModel {

    private static Evaluator evaluator;

    public static void loadModel() throws JAXBException, IOException, SAXException {
        Evaluator newEvaluator = new LoadingModelEvaluatorBuilder()
                .load(new File("/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/training_data/mlp_model.pmml"))
                .build();

        PerformanceModel.evaluator = newEvaluator;
        evaluator.verify();

        List<InputField> inputFields = newEvaluator.getInputFields();
        System.out.println("Input fields: " + inputFields);

        List<TargetField> targetFields = newEvaluator.getTargetFields();
        System.out.println("Target field(s): " + targetFields);

        List<OutputField> outputFields = newEvaluator.getOutputFields();
        System.out.println("Output fields: " + outputFields);
    }

    public static String predictOptimalStrategy(double keySkew, double workloadSkew, double readRatio, double locality, double scopeRatio) {
        Map<FieldName, Object> arguments = new LinkedHashMap<>();
        arguments.put(FieldName.create("keySkew"), keySkew);
        arguments.put(FieldName.create("workloadSkew"), workloadSkew);
        arguments.put(FieldName.create("readRatio"), readRatio);
        arguments.put(FieldName.create("locality"), locality);
        arguments.put(FieldName.create("scopeRatio"), scopeRatio);

        Map<FieldName, ?> results = evaluator.evaluate(arguments);

        Object optimalStrategyResult = results.get(FieldName.create("optimal_strategy"));
        String label = "Prediction failed";

        if (optimalStrategyResult != null) {
            String predictedLabel = optimalStrategyResult.toString();
            label = predictedLabel.substring(predictedLabel.indexOf("result=") + 7, predictedLabel.indexOf(", probability_entries"));

        } else {
            System.out.println("Prediction failed: 'optimal_strategy' result not found");
        }

        return label;

    }

    public static ModelEvaluator<?> loadPMMLModel(String pmmlFilePath) {
        try (InputStream is = new FileInputStream(new File(pmmlFilePath))) {
            PMML pmml = PMMLUtil.unmarshal(is);

            List<Model> models = pmml.getModels();
            if (models.isEmpty()) {
                throw new IllegalStateException("No models found in the PMML file");
            }
            Model model = models.get(0);

            ModelEvaluatorFactory modelEvaluatorFactory = ModelEvaluatorFactory.newInstance();
            ModelEvaluator<?> modelEvaluator =  modelEvaluatorFactory.newModelEvaluator(pmml, model);
            modelEvaluator.verify();

            return modelEvaluator;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String predictOptimalStrategy(ModelEvaluator<?> modelEvaluator, double keySkew, double workloadSkew, double readRatio, double locality, double scopeRatio) {
        System.out.println("Active fields (input): ");
        modelEvaluator.getActiveFields().forEach(field -> System.out.println(field.getName()));

        System.out.println("Target fields (output): ");
        modelEvaluator.getTargetFields().forEach(field -> System.out.println(field.getName()));

        Map<FieldName, Object> arguments = new LinkedHashMap<>();
        arguments.put(FieldName.create("keySkew"), keySkew);
        arguments.put(FieldName.create("workloadSkew"), workloadSkew);
        arguments.put(FieldName.create("readRatio"), readRatio);
        arguments.put(FieldName.create("locality"), locality);
        arguments.put(FieldName.create("scopeRatio"), scopeRatio);

        Map<FieldName, ?> results = modelEvaluator.evaluate(arguments);

        String targetField = "optimal_strategy";  // This should match the actual target field in the PMML file.
        if (results.containsKey(FieldName.create(targetField))) {
            Object predictedValue = results.get(FieldName.create(targetField));
            return predictedValue != null ? predictedValue.toString() : "Prediction failed";
        } else if (results.containsKey(FieldName.create("y"))) {
            Object predictedValue = results.get(FieldName.create("y"));
            return predictedValue != null ? predictedValue.toString() : "Prediction failed";
        } else {
            return "Prediction failed: Target field not found";
        }
    }


}
