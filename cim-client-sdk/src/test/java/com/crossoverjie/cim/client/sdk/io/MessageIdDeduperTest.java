package com.crossoverjie.cim.client.sdk.io;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MessageIdDeduperTest {

    @Test
    public void duplicateReturnsTrueAndRemembers() {
        MessageIdDeduper deduper = new MessageIdDeduper(4);
        Assertions.assertFalse(deduper.checkAndRemember(1L));
        Assertions.assertTrue(deduper.checkAndRemember(1L));
        Assertions.assertFalse(deduper.checkAndRemember(2L));
    }

    @Test
    public void nullIdNotDuplicate() {
        MessageIdDeduper deduper = new MessageIdDeduper();
        Assertions.assertFalse(deduper.checkAndRemember(null));
        Assertions.assertFalse(deduper.checkAndRemember(null));
    }
}
