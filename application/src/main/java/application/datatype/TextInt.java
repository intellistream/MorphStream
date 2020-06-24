/*
 * chombo: Hadoop Map Reduce utility
 * Author: Pranab Ghosh
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package application.datatype;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * @author pranab
 */
class TextInt implements WritableComparable<TextInt> {
    private Text first;
    private IntWritable second;

    public TextInt() {
        first = new Text();
        second = new IntWritable();
    }

    public void set(String first, int second) {
        this.first.set(first);
        this.second.set(second);
    }

    private Text getFirst() {
        return first;
    }

    public void setFirst(Text first) {
        this.first = first;
    }

    private IntWritable getSecond() {
        return second;
    }

    public void setSecond(IntWritable second) {
        this.second = second;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);

    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);

    }

    @Override
    public int compareTo(TextInt other) {
        int cmp = first.compareTo(other.getFirst());
        if (0 == cmp) {
            cmp = second.compareTo(other.getSecond());
        }
        return cmp;
    }

    public int baseCompareTo(TextInt other) {
        return first.compareTo(other.getFirst());
    }

    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode();
    }

    public int baseHashCode() {
        return Math.abs(first.hashCode());
    }

    public boolean equals(Object obj) {
        boolean isEqual = false;
        if (obj instanceof TextInt) {
            TextInt other = (TextInt) obj;
            isEqual = first.equals(other.first) && second.equals(other.second);
        }

        return isEqual;
    }

    public String toString() {
        return first.toString() + ":" + second.get();
    }


}
