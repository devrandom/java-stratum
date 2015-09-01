package org.smartwallet.stratum;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by devrandom on 2015-Aug-25.
 */
public class StratumMessageTest {
    ObjectMapper mapper = new ObjectMapper();

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void deserialize() throws Exception {
        StratumMessage m1 = readValue("{\"id\":123, \"method\":\"a.b\", \"params\":[1, \"x\", null]}");
        assertEquals(123L, (long)m1.id);
        assertEquals("a.b", m1.method);
        assertEquals(m1.params, Lists.newArrayList(1, "x", null));
        StratumMessage m2 = readValue("{\"id\":123, \"result\":{\"x\": 123}}");
        assertTrue(m2.isResult());
        assertEquals(123L, (long)m2.id);
        Map<String, Integer> expected2 = Maps.newTreeMap();
        expected2.put("x", 123);
        //noinspection AssertEqualsBetweenInconvertibleTypes
        assertEquals(expected2, m2.result);

        StratumMessage m3 = readValue("{\"id\":123, \"result\":[\"x\"]}");
        assertEquals(123L, (long)m3.id);
        //noinspection AssertEqualsBetweenInconvertibleTypes
        assertEquals(Lists.newArrayList("x"), m3.result);
    }

    @Test
    public void serializeLineFeed() throws JsonProcessingException {
        assertEquals(mapper.writeValueAsString(new Integer[]{123}), "[123]");
    }

    private StratumMessage readValue(String content) throws java.io.IOException {
        return mapper.readValue(content, StratumMessage.class);
    }
}