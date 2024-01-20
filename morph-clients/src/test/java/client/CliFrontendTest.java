package client;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple cli.App.
 */
public class CliFrontendTest
        extends TestCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public CliFrontendTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(CliFrontendTest.class);
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp() {
        assertTrue(true);
    }
}
