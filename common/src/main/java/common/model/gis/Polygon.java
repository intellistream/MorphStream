package common.model.gis;
import java.util.ArrayList;
public class Polygon {
    private static final int EARTH_RADIUS = 6378137;
    private ArrayList<Point> points;
    private double xmin;
    private double xmax;
    private double ymin;
    private double ymax;
    private int count;
    private double distance_min = 10 / 111.2 * 1000;
    public Polygon(ArrayList<Point> points) {
        this.points = points;
        this.count = points.size();
        this.xmin = Double.MAX_VALUE;
        this.xmax = Double.MIN_VALUE;
        this.ymin = Double.MAX_VALUE;
        this.ymax = Double.MIN_VALUE;
        for (Point p : points) {
            if (p.getX() > this.xmax)
                this.xmax = p.getX();
            if (p.getX() < this.xmin)
                this.xmin = p.getX();
            if (p.getY() > this.ymax)
                this.ymax = p.getY();
            if (p.getY() < this.ymin)
                this.ymin = p.getY();
        }
    }
    public static double pointToLine(double x1, double y1, double x2, double y2, double x0, double y0) {
        double space = 0;
        double a, b, c;
        a = Polygon.lineSpace(x1, y1, x2, y2);
        b = lineSpace(x1, y1, x0, y0);
        c = lineSpace(x2, y2, x0, y0);
        if (c <= 0.000001 || b <= 0.000001) {
            space = 0;
            return space;
        }
        if (a <= 0.000001) {
            space = b;
            return space;
        }
        if (c * c >= a * a + b * b) {
            space = b;
            return space;
        }
        if (b * b >= a * a + c * c) {
            space = c;
            return space;
        }
        double p = (a + b + c) / 2;
        double s = Math.sqrt(p * (p - a) * (p - b) * (p - c));
        space = 2 * s / a;
        return space;
    }
    public static double lineSpace(double x1, double y1, double x2, double y2) {
        double lineLength = 0;
        lineLength = Math.sqrt((x1 - x2) * (x1 - x2) + (y1 - y2)
                * (y1 - y2));
        return lineLength;
    }
    public static double DistancePointToLine(double x1, double y1, double x2, double y2, double x, double y) {
        if (y1 == y2) {
            if (Math.min(x1, x2) < x && Math.max(x1, x2) > x) {
                return Math.abs(ComputeD(y1, x, y, x));
            } else {
                return Math.min(ComputeD(y, x, y1, x1), ComputeD(y, x, y2, x2));
            }
        }
        if (x1 == x2) {
            if (Math.min(y1, y2) < y && Math.max(y1, y2) > y) {
                return ComputeD(y, x1, y, x);
            } else {
                return Math.min(ComputeD(y, x, y1, x1), ComputeD(y, x, y2, x2));
            }
        } else {
            double k = (y2 - y1) / (x2 - x1);
            double tempX = (Math.pow(k, 2.0) * x1 + k * (y - y1) + x) / (Math.pow(k, 2.0) + 1.0);
            double tempY = k * (tempX - x1) + y1;
            if (tempX < -180 || tempX > 180 || tempY < -90 || tempY > 90) {
                return Math.min(ComputeD(y, x, y1, x1), ComputeD(y, x, y2, x2));
            }
            double tempDis1 = (ComputeD(tempY, tempX, y1, x1) + ComputeD(tempY, tempX, y2, x2));
            double tempDis2 = ComputeD(y1, x1, y2, x2);
            if ((tempDis1 - tempDis2) < 0.001) {
                return (ComputeD(tempY, tempX, y, x));
            } else {
                return Math.min(ComputeD(y, x, y1, x1), ComputeD(y, x, y2, x2));
            }
        }
    }
    public static double ComputeD(double lat_a, double lng_a, double lat_b, double lng_b) {
        double radLat1 = (lat_a * Math.PI / 180.0);
        double radLat2 = (lat_b * Math.PI / 180.0);
        double a = radLat1 - radLat2;
        double b = (lng_a - lng_b) * Math.PI / 180.0;
        double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2)
                + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)));
        s = s * EARTH_RADIUS;
        return s;
    }
    public Boolean contains(Point p) {
        if (p.getX() >= xmax || p.getX() < xmin || p.getY() >= ymax || p.getY() < ymin)
            return false;
        int cn = 0;
        int n = points.size();
        for (int i = 0; i < n - 1; i++) {
            if (points.get(i).getY() != points.get(i + 1).getY() && !((p.getY() < points.get(i).getY())
                    && (p.getY() < points.get(i + 1).getY())) && !((p.getY() > points.get(i).getY())
                    && (p.getY() > points.get(i + 1).getY()))) {
                double uy = 0;
                double by = 0;
                double ux = 0;
                double bx = 0;
                int dir = 0;
                if (points.get(i).getY() > points.get(i + 1).getY()) {
                    uy = points.get(i).getY();
                    by = points.get(i + 1).getY();
                    ux = points.get(i).getX();
                    bx = points.get(i + 1).getX();
                    dir = 0;//downward
                } else {
                    uy = points.get(i + 1).getY();
                    by = points.get(i).getY();
                    ux = points.get(i + 1).getX();
                    bx = points.get(i).getX();
                    dir = 1;//upward
                }
                double tx = 0;
                if (ux != bx) {
                    double k = (uy - by) / (ux - bx);
                    double b = ((uy - k * ux) + (by - k * bx)) / 2;
                    tx = (p.getY() - b) / k;
                } else {
                    tx = ux;
                }
                if (tx > p.getX()) {
                    if (dir == 1 && p.getY() != points.get(i + 1).getY())
                        cn++;
                    else if (p.getY() != points.get(i).getY())
                        cn++;
                }
            }
        }
        return cn % 2 != 0;
    }
    public boolean matchToRoad(Point p, int roadWidth) {
        int n = points.size();
        for (int i = 0; i < n - 1; i++) {
            double distance = Math.sqrt(Math.pow(points.get(i).getY() - p.getY(), 2) + Math.pow(points.get(i).getX() - p.getX(), 2));
            if (distance < roadWidth / 2.0 * Math.sqrt(2.0))
                return true;
        }
        return false;
    }
    public boolean matchToRoad(Point p, int roadWidth, ArrayList<Point> ps) {
        double minD = Double.MAX_VALUE;
        int n = ps.size();
        for (int i = 0; i < n - 2; i++) {
            double distance = Polygon.pointToLine(ps.get(i).getX(), ps.get(i).getY(), ps.get(i + 1).getX(), ps.get(i + 1).getY(), p.getX(), p.getY()) * 111.2 * 1000;
            if (distance < minD)
                minD = distance;
            //System.out.println("distance="+distance);
            if (distance < roadWidth / 2.0 * Math.sqrt(2.0))
                return true;
        }
        return false;
    }
}
