package com.crossoverjie.cim.client.sdk.io;

import com.crossoverjie.cim.common.constant.Constants;
import com.crossoverjie.cim.common.protocol.BaseCommand;
import com.crossoverjie.cim.common.protocol.Response;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ReliableDeliveryDispatcherTest {

    @Test
    public void alignedBatchCallbacksAndAcksEachIndex() {
        Response msg = Response.newBuilder()
                .setCmd(BaseCommand.OFFLINE)
                .putProperties(Constants.MetaKey.RECEIVE_USER_ID, "1")
                .addBatchResMsg("a")
                .addBatchResMsg("b")
                .addBatchResMsg("c")
                .addBatchMessageId(11L)
                .addBatchMessageId(12L)
                .addBatchMessageId(13L)
                .build();

        ReliableDeliveryDispatcher.DispatchPlan plan =
                ReliableDeliveryDispatcher.plan(msg, new MessageIdDeduper());

        Assertions.assertFalse(plan.isInvalid());
        Assertions.assertEquals(3, plan.getItems().size());
        assertItem(plan.getItems().get(0), 11L, "a", false, true, true);
        assertItem(plan.getItems().get(1), 12L, "b", false, true, true);
        assertItem(plan.getItems().get(2), 13L, "c", false, true, true);
    }

    @Test
    public void duplicateIdsSkipCallbackButStillAck() {
        MessageIdDeduper deduper = new MessageIdDeduper();
        Response first = aligned("hello", 99L);
        ReliableDeliveryDispatcher.plan(first, deduper);

        ReliableDeliveryDispatcher.DispatchPlan replay =
                ReliableDeliveryDispatcher.plan(aligned("hello", 99L), deduper);

        Assertions.assertFalse(replay.isInvalid());
        Assertions.assertEquals(1, replay.getItems().size());
        assertItem(replay.getItems().get(0), 99L, "hello", true, false, true);
    }

    @Test
    public void mismatchedLengthIsInvalidNoCallbackNoAck() {
        Response msg = Response.newBuilder()
                .addBatchResMsg("a")
                .addBatchResMsg("b")
                .addBatchMessageId(1L)
                .build();

        ReliableDeliveryDispatcher.DispatchPlan plan =
                ReliableDeliveryDispatcher.plan(msg, new MessageIdDeduper());

        Assertions.assertTrue(plan.isInvalid());
        Assertions.assertTrue(plan.getItems().isEmpty());
        Assertions.assertFalse(ReliableDeliveryDispatcher.isValidBatch(2, 1));
    }

    @Test
    public void idsWithoutBodiesIsInvalid() {
        Response msg = Response.newBuilder()
                .addBatchMessageId(1L)
                .addBatchMessageId(2L)
                .setResMsg("should-not-use")
                .putProperties(Constants.MetaKey.MESSAGE_ID, "1")
                .build();

        ReliableDeliveryDispatcher.DispatchPlan plan =
                ReliableDeliveryDispatcher.plan(msg, new MessageIdDeduper());

        Assertions.assertTrue(plan.isInvalid());
        Assertions.assertTrue(plan.getItems().isEmpty());
    }

    @Test
    public void emptyBatchCountsAreNotValidBatch() {
        Assertions.assertFalse(ReliableDeliveryDispatcher.isValidBatch(0, 0));
        Assertions.assertFalse(ReliableDeliveryDispatcher.isValidBatch(3, 0));
        Assertions.assertTrue(ReliableDeliveryDispatcher.isValidBatch(2, 2));
    }

    @Test
    public void singleUsesPropertiesMessageId() {
        Response msg = Response.newBuilder()
                .setResMsg("hi")
                .putProperties(Constants.MetaKey.MESSAGE_ID, "7")
                .build();

        ReliableDeliveryDispatcher.DispatchPlan plan =
                ReliableDeliveryDispatcher.plan(msg, new MessageIdDeduper());

        Assertions.assertFalse(plan.isInvalid());
        Assertions.assertEquals(1, plan.getItems().size());
        assertItem(plan.getItems().get(0), 7L, "hi", false, true, true);
    }

    @Test
    public void singleDuplicateSkipsCallbackAndAcks() {
        MessageIdDeduper deduper = new MessageIdDeduper();
        Response msg = Response.newBuilder()
                .setResMsg("hi")
                .putProperties(Constants.MetaKey.MESSAGE_ID, "7")
                .build();
        ReliableDeliveryDispatcher.plan(msg, deduper);

        ReliableDeliveryDispatcher.DispatchPlan replay =
                ReliableDeliveryDispatcher.plan(msg, deduper);
        assertItem(replay.getItems().get(0), 7L, "hi", true, false, true);
    }

    @Test
    public void legacyBodiesWithoutIdsStillCallback() {
        Response msg = Response.newBuilder()
                .addBatchResMsg("a")
                .addBatchResMsg("b")
                .putProperties(Constants.MetaKey.MESSAGE_ID, "only-one")
                .build();

        ReliableDeliveryDispatcher.DispatchPlan plan =
                ReliableDeliveryDispatcher.plan(msg, new MessageIdDeduper());

        Assertions.assertFalse(plan.isInvalid());
        List<ReliableDeliveryDispatcher.DispatchItem> items = plan.getItems();
        Assertions.assertEquals(3, items.size());
        Assertions.assertTrue(items.get(0).shouldCallback());
        Assertions.assertFalse(items.get(0).shouldAck());
        Assertions.assertEquals("a", items.get(0).getBody());
        Assertions.assertEquals("b", items.get(1).getBody());
        Assertions.assertFalse(items.get(2).shouldCallback());
        Assertions.assertTrue(items.get(2).shouldAck());
        Assertions.assertEquals("only-one", items.get(2).getAckMessageId());
    }

    private static Response aligned(String body, long messageId) {
        return Response.newBuilder()
                .addBatchResMsg(body)
                .addBatchMessageId(messageId)
                .build();
    }

    private static void assertItem(ReliableDeliveryDispatcher.DispatchItem item,
                                   Long messageId, String body,
                                   boolean duplicate, boolean callback, boolean ack) {
        Assertions.assertEquals(messageId, item.getMessageId());
        Assertions.assertEquals(body, item.getBody());
        Assertions.assertEquals(duplicate, item.isDuplicate());
        Assertions.assertEquals(callback, item.shouldCallback());
        Assertions.assertEquals(ack, item.shouldAck());
        if (ack && messageId != null) {
            Assertions.assertEquals(String.valueOf(messageId), item.getAckMessageId());
        }
    }
}
