package com.ljw4dakeai.pojo;

public class Score {
    String scoreName;
    int score;

    public Score(String scoreName, int score) {
        this.scoreName = scoreName;
        this.score = score;
    }

    public String getScoreName() {
        return scoreName;
    }

    public void setScoreName(String scoreName) {
        this.scoreName = scoreName;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }
}
