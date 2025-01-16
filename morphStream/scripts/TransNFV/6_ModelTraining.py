import argparse
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPClassifier
from sklearn2pmml import PMMLPipeline, sklearn2pmml
from sklearn.metrics import accuracy_score


def model_training(exp_dir):
    data = pd.read_csv(f"{exp_dir}/training_data/optimal_modules.csv", header=None)
    data.columns = ['keySkew', 'workloadSkew', 'readRatio', 'locality', 'scopeRatio', 'optimal_strategy']# keySkew, workloadSkew, readRatio, locality, scopeRatio, optimal_strategy

    X = data[['keySkew', 'workloadSkew', 'readRatio', 'locality', 'scopeRatio']]
    y = data['optimal_strategy']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    mlp = MLPClassifier(hidden_layer_sizes=(64, 32), max_iter=500, random_state=42)
    mlp.fit(X_train, y_train.values.ravel())

    y_pred = mlp.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    print(f"Test Accuracy: {accuracy:.4f}")
    pipeline = PMMLPipeline([
        ("classifier", mlp)
    ])

    pipeline.target_fields = ["optimal_strategy"]
    pipeline.active_fields = ["keySkew", "workloadSkew", "readRatio", "locality", "scopeRatio"]

    # Export the PMML model
    sklearn2pmml(pipeline, f"{exp_dir}/training_data/mlp_model.pmml", with_repr=True)



def main(root_dir, exp_dir):
    print(f"Root directory: {root_dir}")
    print(f"Experiment directory: {exp_dir}")
    model_training(exp_dir)

    print("Done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process the root directory.")
    parser.add_argument('--root_dir', type=str, required=True, help="Root directory path")
    parser.add_argument('--exp_dir', type=str, required=True, help="Experiment directory path")
    args = parser.parse_args()
    main(args.root_dir, args.exp_dir)
    print("MLP model trained and exported.")