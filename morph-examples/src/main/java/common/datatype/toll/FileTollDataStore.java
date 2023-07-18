/*
 * #!
 * %
 * Copyright (C) 2014 - 2015 Humboldt-Universität zu Berlin
 * %
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #_
 */
package common.datatype.toll;

import common.bolts.model.TollEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.LinkedList;
import java.util.Queue;

/**
 * A {@link TollDataStore} that uses a file for storage including all disadvantages. Use only in a very simple topology
 * as the file I/O can easily produce a performance bottleneck.
 *
 * @author richter
 */
public class FileTollDataStore implements TollDataStore {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(FileTollDataStore.class);
    private final File histFile;
    private boolean firstWriteDone = false;

    /**
     * Creates a {@code FileTollDataStore} using a temporary file
     *
     * @throws IOException if the creation of the temporary file fails
     */
    private FileTollDataStore() throws IOException {
        this.histFile = File.createTempFile("aeolus-lrb", null);
    }

    /**
     * Creates a {@code FileTollDataStore} reading from {@code histFile}.
     *
     * @param histFile
     */
    private FileTollDataStore(File histFile) {
        this.histFile = histFile;
    }

    /**
     * @param histFilePath
     * @throws FileNotFoundException    if the file denoted by {@code histFilePath} doesn't exist
     * @throws IllegalArgumentException if {@code histFilePath} is {@code null} or empty.
     */
    private FileTollDataStore(String histFilePath) {
        if (histFilePath == null) {
            throw new IllegalArgumentException("histFilePath mustn't be null.");
        }
        if (histFilePath.isEmpty()) {
            throw new IllegalArgumentException("no filename for historic data given.");
        }
        this.histFile = new File(histFilePath);
    }

    @Override
    public Integer retrieveToll(int xWay, int day, int vehicleIdentifier) {
        try {
            FileInputStream is = new FileInputStream(histFile);
            ObjectInputStream objectInputStream = new ObjectInputStream(is);
            Object nextObject = objectInputStream.readObject();
            while (nextObject != null) {
                TollEntry next = (TollEntry) nextObject;
                if (next.getxWay() == xWay && next.getADay() == day && next.getVehicleIdentifier() == vehicleIdentifier) {
                    return next.getToll();
                }
            }
            objectInputStream.close();
            is.close();
        } catch (IOException | ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }
        return null;
    }

    @Override
    public void storeToll(int xWay, int day, int vehicleIdentifier, int toll) {
        try {
            if (!firstWriteDone) {
                FileOutputStream os = new FileOutputStream(histFile, true // append
                );
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(os);
                objectOutputStream.writeObject(null);
                objectOutputStream.flush();
                objectOutputStream.close();
                os.flush();
                os.close();
                firstWriteDone = true;
            }
            FileInputStream is = new FileInputStream(histFile);
            ObjectInputStream objectInputStream = new ObjectInputStream(is);
            File tmpStoreFile = File.createTempFile("aeolus-lrb", null);
            FileOutputStream os = new FileOutputStream(tmpStoreFile);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(os);
            Object nextObject = objectInputStream.readObject();
            Queue<Object> tmpStore = new LinkedList<>();
            while (nextObject != null) {
                tmpStore.add(nextObject);
                if (tmpStore.size() > 1000) {
                    while (!tmpStore.isEmpty()) {
                        objectOutputStream.writeObject(tmpStore.poll());
                    }
                }
                nextObject = objectInputStream.readObject();
            }
            TollEntry newTollEntry = new TollEntry(vehicleIdentifier, xWay, day, toll);
            objectOutputStream.writeObject(newTollEntry);
            objectOutputStream.writeObject(null);
            objectInputStream.close();
            objectOutputStream.close();
            is.close();
            os.close();
            histFile.delete();
            tmpStoreFile.renameTo(histFile);
        } catch (IOException | ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Integer removeEntry(int xWay, int day, int vehicleIdentifier) {
        Integer retValue = null;
        try {
            FileInputStream is = new FileInputStream(histFile);
            ObjectInputStream objectInputStream = new ObjectInputStream(is);
            File tmpStoreFile = File.createTempFile("aeolus-lrb", null);
            FileOutputStream os = new FileOutputStream(tmpStoreFile);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(os);
            Object nextObject = objectInputStream.readObject();
            Queue<Object> tmpStore = new LinkedList<>();
            while (nextObject != null) {
                TollEntry next = (TollEntry) nextObject;
                if (!(next.getxWay() == xWay && next.getADay() == day && next.getVehicleIdentifier() == vehicleIdentifier)) {
                    tmpStore.add(nextObject);
                    if (tmpStore.size() > 1000) {
                        while (!tmpStore.isEmpty()) {
                            objectOutputStream.writeObject(tmpStore.poll());
                        }
                    }
                } else {
                    retValue = next.getToll();
                }
                nextObject = objectInputStream.readObject();
            }
            objectOutputStream.writeObject(null);
            objectInputStream.close();
            objectOutputStream.close();
            is.close();
            os.close();
            histFile.delete();
            tmpStoreFile.renameTo(histFile);
        } catch (IOException | ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }
        return retValue;
    }
}
