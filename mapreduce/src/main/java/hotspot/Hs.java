package hotspot;

/** Shared constants and pure helper functions (unit-testable). */
public final class Hs {
    private Hs() {}

    public static final String ASOF   = "hotspot.asof";          // yyyy-MM-dd
    public static final String WINDOW = "hotspot.window.days";   // default 90
    public static final String CELL   = "hotspot.cell.deg";     // default 0.02
    public static final String HALF   = "hotspot.halflife.days"; // default 30
    public static final String TOPN   = "hotspot.topn";          // default 20

    /** Time band from the hour of day (0-23). */
    public static String band(int hour) {
        if (hour >= 7 && hour <= 9)   return "AM_PEAK";
        if (hour >= 16 && hour <= 18) return "PM_PEAK";
        if (hour >= 19 || hour < 5)   return "NIGHT";
        return "DAY";
    }

    /** STATS19 severity: 1 fatal, 2 serious, 3 slight. */
    public static double severityWeight(int severity) {
        switch (severity) {
            case 1:  return 10.0;
            case 2:  return 5.0;
            default: return 1.0;
        }
    }
}
