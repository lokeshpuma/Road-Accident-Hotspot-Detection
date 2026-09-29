package hotspot;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class HsTest {
    @Test
    public void testBand() {
        assertEquals("NIGHT", Hs.band(4));
        assertEquals("DAY", Hs.band(5));
        assertEquals("AM_PEAK", Hs.band(7));
        assertEquals("AM_PEAK", Hs.band(9));
        assertEquals("DAY", Hs.band(10));
        assertEquals("PM_PEAK", Hs.band(16));
        assertEquals("PM_PEAK", Hs.band(18));
        assertEquals("NIGHT", Hs.band(19));
    }

    @Test
    public void testSeverityWeight() {
        assertEquals(10.0, Hs.severityWeight(1), 1e-6);
        assertEquals(5.0, Hs.severityWeight(2), 1e-6);
        assertEquals(1.0, Hs.severityWeight(3), 1e-6);
    }
}
