import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPClassifier
from sklearn2pmml import PMMLPipeline, sklearn2pmml
from sklearn.metrics import accuracy_score

# Step 1: Load the labeled training data from CSV file
data = pd.read_csv('/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/training_data/combined_optimal_strategies.csv', header=None)

# Manually assign column names
data.columns = ['keySkew', 'workloadSkew', 'readRatio', 'locality', 'scopeRatio', 'optimal_strategy']

# Split the data into features (X) and labels (y)
X = data[['keySkew', 'workloadSkew', 'readRatio', 'locality', 'scopeRatio']]  # 5-tuple of workload characteristics
y = data['optimal_strategy']  # Optimal strategy label

# Step 2: Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Step 3: Define and train the MLP model
mlp = MLPClassifier(hidden_layer_sizes=(64, 32), max_iter=500, random_state=42)
mlp.fit(X_train, y_train.values.ravel())  # Use .ravel() to convert y_train into a 1D array

# Step 4: Evaluate the model on the testing data
y_pred = mlp.predict(X_test)  # Predict labels for the testing set
accuracy = accuracy_score(y_test, y_pred)  # Calculate accuracy
print(f"Test Accuracy: {accuracy:.4f}")

# Step 5: Export the trained model to a PMML file, explicitly setting the target field name
pipeline = PMMLPipeline([
    ("classifier", mlp)
])

# Explicitly set the target field to "optimal_strategy" and active fields to your feature names
pipeline.target_fields = ["optimal_strategy"]
pipeline.active_fields = ["keySkew", "workloadSkew", "readRatio", "locality", "scopeRatio"]

# Export the PMML model
sklearn2pmml(pipeline, "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV/training_data/mlp_model.pmml", with_repr=True)
