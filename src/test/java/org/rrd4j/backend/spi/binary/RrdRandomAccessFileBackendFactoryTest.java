package org.rrd4j.backend.spi.binary;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;
import org.rrd4j.backend.spi.binary.RrdRandomAccessFileBackendFactory;
import org.rrd4j.core.BackEndFactoryTest;
import org.rrd4j.core.RrdBackendFactory;


public class RrdRandomAccessFileBackendFactoryTest extends BackEndFactoryTest {

    @Override
    @Test
    public void testName() {
        checkRegistred("FILE", RrdRandomAccessFileBackendFactory.class);
        
    }

    @Override
    @Test
    public void testBeans() throws IntrospectionException {
        checkBeans(RrdRandomAccessFileBackendFactory.class);
    }

    @Test
    public void testStat() throws IOException {
        RrdBackendFactory factory = RrdBackendFactory.getFactory("FILE");
        
        factory.start();
        Map<String, Number> stats = getStats(factory, "truc.rrd");
        Assert.assertEquals(0, stats.size());
    }

}