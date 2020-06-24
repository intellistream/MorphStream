package application.model.predictor;

import java.io.Serializable;

public class Prediction implements Serializable {
    private static final long serialVersionUID = -669815404243309408L;
    private char[] entityId;
    private double score;
    private String[] states;
    private boolean outlier;

    public Prediction(char[] entityId, double score, String[] states, boolean outlier) {
        this.entityId = entityId;
        this.score = score;
        this.states = states;
        this.outlier = outlier;
    }

    public char[] getEntityId() {
        return entityId;
    }

    public void setEntityId(char[] entityId) {
        this.entityId = entityId;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public String[] getStates() {
        return states;
    }

    public void setStates(String[] states) {
        this.states = states;
    }

    public boolean isOutlier() {
        return outlier;
    }

    public void setOutlier(boolean outlier) {
        this.outlier = outlier;
    }
}