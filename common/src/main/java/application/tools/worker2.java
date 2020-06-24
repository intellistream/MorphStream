package application.tools;

/**
 * Created by szhang026 on 5/30/2016.
 */
public class worker2 {
    final Control control = new Control();

    public static void main(String[] args) {
        try {
            worker2 test = new worker2();
            test.test();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void test() {
        mapMatcher main = new mapMatcher();
        speedCal help = new speedCal();

        new Thread(main).start();
        new Thread(help).start();
    }

    class Control {
        public volatile boolean flag = false;
    }

    class mapMatcher implements Runnable {
        @Override
        public void run() {
            while (!control.flag) {

            }
        }
    }

    class speedCal implements Runnable {
        @Override
        public void run() {
            while (!control.flag) {

            }
        }
    }
}