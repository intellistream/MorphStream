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
        // Building a model evaluator from a PMML file
        Evaluator newEvaluator = new LoadingModelEvaluatorBuilder()
                .load(new File("/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/training_data/mlp_model.pmml"))
                .build();

        PerformanceModel.evaluator = newEvaluator;
        // Perforing the self-check
        evaluator.verify();

        // Printing input (x1, x2, .., xn) fields
        List<InputField> inputFields = newEvaluator.getInputFields();
        System.out.println("Input fields: " + inputFields);

        // Printing primary result (y) field(s)
        List<TargetField> targetFields = newEvaluator.getTargetFields();
        System.out.println("Target field(s): " + targetFields);

        // Printing secondary result (eg. probability(y), decision(y)) fields
        List<OutputField> outputFields = newEvaluator.getOutputFields();
        System.out.println("Output fields: " + outputFields);
    }

    public static String testUsage(double keySkew, double workloadSkew, double readRatio, double locality, double scopeRatio) throws JAXBException, IOException, SAXException {
        // Prepare input fields (5-tuple)
        Map<FieldName, Object> arguments = new LinkedHashMap<>();
        arguments.put(FieldName.create("keySkew"), keySkew);
        arguments.put(FieldName.create("workloadSkew"), workloadSkew);
        arguments.put(FieldName.create("readRatio"), readRatio);
        arguments.put(FieldName.create("locality"), locality);
        arguments.put(FieldName.create("scopeRatio"), scopeRatio);

        // Access the map containing the results
        Map<FieldName, ?> results = evaluator.evaluate(arguments);

        // Retrieve the 'optimal_strategy' entry from the results map
        Object optimalStrategyResult = results.get(FieldName.create("optimal_strategy"));
        String label = "Prediction failed";

        // Ensure it's not null before proceeding
        if (optimalStrategyResult != null) {
            // Cast the result to the correct type (in this case, looks like a complex object)
            String predictedLabel = optimalStrategyResult.toString();  // This will give you the full object string

            // To get just the predicted label (i.e., "Offloading"), you can extract it from the result string
            // Since the predicted label is present after "result=", you can parse it
            label = predictedLabel.substring(predictedLabel.indexOf("result=") + 7, predictedLabel.indexOf(", probability_entries"));

//            System.out.println("Predicted Label: " + label);
        } else {
            System.out.println("Prediction failed: 'optimal_strategy' result not found");
        }

        return label;

    }

    public static ModelEvaluator<?> loadPMMLModel(String pmmlFilePath) {
        try (InputStream is = new FileInputStream(new File(pmmlFilePath))) {
            // Load the PMML file
            PMML pmml = PMMLUtil.unmarshal(is);

            // Extract the first model from the PMML file
            List<Model> models = pmml.getModels();
            if (models.isEmpty()) {
                throw new IllegalStateException("No models found in the PMML file");
            }
            Model model = models.get(0);  // Get the first model

            // Create the ModelEvaluator using the PMML and the extracted model
            ModelEvaluatorFactory modelEvaluatorFactory = ModelEvaluatorFactory.newInstance();
            ModelEvaluator<?> modelEvaluator =  modelEvaluatorFactory.newModelEvaluator(pmml, model);
            modelEvaluator.verify(); // Verifies that the evaluator is properly initialized

            return modelEvaluator;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String predictOptimalStrategy(ModelEvaluator<?> modelEvaluator, double keySkew, double workloadSkew, double readRatio, double locality, double scopeRatio) {
        // Debugging: Print active and target fields
        System.out.println("Active fields (input): ");
        modelEvaluator.getActiveFields().forEach(field -> System.out.println(field.getName()));

        System.out.println("Target fields (output): ");
        modelEvaluator.getTargetFields().forEach(field -> System.out.println(field.getName()));

        // Prepare input fields (5-tuple)
        Map<FieldName, Object> arguments = new LinkedHashMap<>();
        arguments.put(FieldName.create("keySkew"), keySkew);
        arguments.put(FieldName.create("workloadSkew"), workloadSkew);
        arguments.put(FieldName.create("readRatio"), readRatio);
        arguments.put(FieldName.create("locality"), locality);
        arguments.put(FieldName.create("scopeRatio"), scopeRatio);

        // Evaluate the model with the input data
        Map<FieldName, ?> results = modelEvaluator.evaluate(arguments);

        // Check if the correct target field is being used
        String targetField = "optimal_strategy";  // This should match the actual target field in the PMML file.
        if (results.containsKey(FieldName.create(targetField))) {
            Object predictedValue = results.get(FieldName.create(targetField));
            return predictedValue != null ? predictedValue.toString() : "Prediction failed";
        } else if (results.containsKey(FieldName.create("y"))) {
            // If the target field is "y", use it instead
            Object predictedValue = results.get(FieldName.create("y"));
            return predictedValue != null ? predictedValue.toString() : "Prediction failed";
        } else {
            return "Prediction failed: Target field not found";
        }
    }


}
