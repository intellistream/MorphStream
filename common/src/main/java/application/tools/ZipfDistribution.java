package application.tools;

/**
 * Created by I309939 on 7/29/2016.
 */

public class ZipfDistribution {
    private int N; // number
    private int s; // screw

    public ZipfDistribution(int size, int skew) {
        this.N = size;
        this.s = skew;
    }

    public static void main(String[] args) {
//        if(args.length != 2) {
//            System.out.println("usage: ./zipf N s");
//            System.exit(-1);
//        }

        int n = 10;
        int s = 0;

        ZipfDistribution z = new ZipfDistribution(n, s);

        StringBuilder output = new StringBuilder("frequency\tcdf\n");
        for (int i = 1; i <= n; i++) {
            output.append(z.f(i)).append("\t").append(z.cdf(i)).append("\n");
        }
        System.out.println(output);
    }

    public double H(int n, int s) { // Harmonic number
        if (n == 1) {
            return 1.0 / Math.pow(n, s);
        } else {
            return (1.0 / Math.pow(n, s)) + H(n - 1, s);
        }
    }

    public double f(int k) {
        return (1 / Math.pow(k, this.s)) / H(this.N, this.s);
    }

    public double cdf(int k) {
        return H(k, this.s) / H(this.N, this.s);
    }
}