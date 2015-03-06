package org.rrd4j.core;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rrd4j.ConsolFun;
import org.rrd4j.DsType;
import org.rrd4j.core.RrdDb;
import org.rrd4j.core.RrdDef;
import org.rrd4j.core.Sample;

/**
 * @author kunilov.p
 *
 */
public class TestIssue43 {

        @Rule
        public TemporaryFolder testFolder = new TemporaryFolder();

        private static final String CONFIGURATION_TYPE = "GAUGE";

        private static final Double MAX_VALUE = Double.NaN;

        private static final Double MIN_VALUE = Double.NaN;

        private static long castDateTimeForRRD(long dateTime, long samplingRate) {
                long seconds = (dateTime + 500L) / 1000L;
                seconds = seconds - (seconds % samplingRate);
                return seconds;
        }

        private RrdDb rrdDb;
        // //////////////////////////////////////////////////////////
        // Settings to configure rrdDb
        // //////////////////////////////////////////////////////////
        private static final long samplingRate = 1;
        private static final long heartbeat = 2 * samplingRate;
        private static final long startDateTime = 1369748048000l;
        private static final short version = 2;
        private static final double xff = 0.999;
        private static final String datasourceName = "test";

        // /////////////////////////////////////////////////////////
        // Settings to configure data to write
        // //////////////////////////////////////////////////////////
        private long data = 1;
        private final long dataCount = 10;
        private final long dataRate = 3;

        @Before
        public void setUp() throws Exception {
                //RrdDb.setDefaultFactory("FILE");
                String rrdfile = testFolder.newFile("TestIssue43.rrd").getCanonicalPath();
                final RrdDef rrdDef = new RrdDef(rrdfile, castDateTimeForRRD(
                        startDateTime, samplingRate), samplingRate, version);

                rrdDef.addDatasource(datasourceName,
                        DsType.valueOf(CONFIGURATION_TYPE), heartbeat, MAX_VALUE,
                        MIN_VALUE);
                rrdDef.addArchive(ConsolFun.MIN, xff, 1, 30);

                rrdDb = new RrdDb(rrdDef);
        }

        @After
        public void tearDown() throws IOException {
                rrdDb.close();
        }

        @Test
        public void testWriteToRRD() throws Exception {
                System.out.println("mkdir -p " + new File(rrdDb.getCanonicalPath()).getParent());
                System.out.println("rrdtool " + rrdDb.getRrdDef().dump().replace("--version 2", ""));
                final long castedStartDateTime = castDateTimeForRRD(startDateTime,
                        samplingRate);
                //long oldDateTime = -1L;
                for (int index = 1; index <= dataCount; index += dataRate) {


                        long newDateTime = castedStartDateTime + samplingRate * index;

                        /*if(oldDateTime != -1 && (newDateTime - oldDateTime) >= heartbeat) {
                                System.out.println("DUBEC did correction");
                                final Sample sample = rrdDb.createSample();
                                sample.setTime(newDateTime - 1);
                                sample.setValue(datasourceName, MIN_VALUE);
                                System.out.println("rrdtool " + sample.dump());
                                sample.update();
                        }*/

                        final Sample sample2 = rrdDb.createSample();
                        sample2.setTime(newDateTime);
                        sample2.setValue(datasourceName, data++);
                        System.out.println("rrdtool " + sample2.dump());
                        sample2.update();
                        //oldDateTime = newDateTime;
                }
                System.out.println("rrdtool dump " + rrdDb.getCanonicalPath());
                System.out.println(rrdDb.exportXml());
        }
}