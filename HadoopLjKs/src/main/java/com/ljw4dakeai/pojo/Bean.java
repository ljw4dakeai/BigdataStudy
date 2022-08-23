package com.ljw4dakeai.pojo;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Bean implements Writable {
    private String num;
    private String name;
    private int math;

    public Bean() {
        super();
    }

    public Bean(String num, String name, int math, int english) {
        this.num = num;
        this.name = name;
        this.math = math;
        this.english = english;
    }

    public String getNum() {
        return num;
    }

    public void setNum(String num) {
        this.num = num;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getMath() {
        return math;
    }

    public void setMath(int math) {
        this.math = math;
    }

    public int getEnglish() {
        return english;
    }

    public void setEnglish(int english) {
        this.english = english;
    }

    private int english;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(num);
        dataOutput.writeUTF(name);
        dataOutput.writeInt(math);
        dataOutput.writeInt(english);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        num = dataInput.readUTF();
        name = dataInput.readUTF();
        math = dataInput.readInt();
        english = dataInput.readInt();

    }
}
