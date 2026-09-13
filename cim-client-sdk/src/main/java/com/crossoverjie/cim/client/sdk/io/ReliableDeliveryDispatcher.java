package com.crossoverjie.cim.client.sdk.io;

import com.crossoverjie.cim.common.constant.Constants;
import com.crossoverjie.cim.common.protocol.Response;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 可靠投递收包规划：批量按下标对齐去重，每条独立确认。
 * <p>
 * 入口：CIMClientHandle 收到非 PING 包。<br>
 * 步骤：合法批量 → 按下标取 id+正文 → Deduper → 回调或跳过 → 每条 MESSAGE_ACK。<br>
 * 失败：正文列表与编号列表长度 0 错位（双方均 &gt; 0 但不相等，或仅有编号无正文）→ 整包不回调、不确认。
 * <p>
 * 确认协议仍为单条 MESSAGE_ACK（一次确认多个编号本任务不做）。
 */
public final class ReliableDeliveryDispatcher {

    private ReliableDeliveryDispatcher() {
    }

    /**
     * @return true 当且仅当正文、编号均 &gt; 0 且等长
     */
    public static boolean isValidBatch(int bodyCount, int idCount) {
        return bodyCount > 0 && idCount > 0 && bodyCount == idCount;
    }

    public static DispatchPlan plan(Response msg, MessageIdDeduper deduper) {
        int bodyCount = msg.getBatchResMsgCount();
        int idCount = msg.getBatchMessageIdCount();

        if (bodyCount > 0 && idCount > 0) {
            if (bodyCount != idCount) {
                return DispatchPlan.invalid();
            }
            return planAlignedBatch(msg, deduper);
        }
        if (idCount > 0) {
            return DispatchPlan.invalid();
        }
        if (bodyCount > 0) {
            return planLegacyBatch(msg);
        }
        return planSingle(msg, deduper);
    }

    private static DispatchPlan planAlignedBatch(Response msg, MessageIdDeduper deduper) {
        List<DispatchItem> items = new ArrayList<>(msg.getBatchResMsgCount());
        for (int i = 0; i < msg.getBatchResMsgCount(); i++) {
            long messageId = msg.getBatchMessageId(i);
            String body = msg.getBatchResMsg(i);
            boolean duplicate = deduper.checkAndRemember(messageId);
            items.add(new DispatchItem(messageId, String.valueOf(messageId), body, duplicate, !duplicate, true));
        }
        return DispatchPlan.valid(items);
    }

    /**
     * 仅有正文、无编号列表：历史批量（在线 P2P batchMsg / 群聊），不走按条可靠投递。
     */
    private static DispatchPlan planLegacyBatch(Response msg) {
        List<DispatchItem> items = new ArrayList<>(msg.getBatchResMsgCount() + 1);
        for (int i = 0; i < msg.getBatchResMsgCount(); i++) {
            items.add(new DispatchItem(null, null, msg.getBatchResMsg(i), false, true, false));
        }
        String messageIdStr = msg.getPropertiesMap().get(Constants.MetaKey.MESSAGE_ID);
        if (messageIdStr != null) {
            items.add(new DispatchItem(parseMessageId(messageIdStr), messageIdStr, null, false, false, true));
        }
        return DispatchPlan.valid(items);
    }

    private static DispatchPlan planSingle(Response msg, MessageIdDeduper deduper) {
        String messageIdStr = msg.getPropertiesMap().get(Constants.MetaKey.MESSAGE_ID);
        Long messageId = parseMessageId(messageIdStr);
        boolean duplicate = messageId != null && deduper.checkAndRemember(messageId);
        boolean ack = messageIdStr != null;
        DispatchItem item = new DispatchItem(messageId, messageIdStr, msg.getResMsg(), duplicate, !duplicate, ack);
        return DispatchPlan.valid(List.of(item));
    }

    static Long parseMessageId(String messageIdStr) {
        if (messageIdStr == null || messageIdStr.isEmpty()) {
            return null;
        }
        try {
            return Long.valueOf(messageIdStr);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static final class DispatchPlan {
        private final boolean invalid;
        private final List<DispatchItem> items;

        private DispatchPlan(boolean invalid, List<DispatchItem> items) {
            this.invalid = invalid;
            this.items = items;
        }

        static DispatchPlan invalid() {
            return new DispatchPlan(true, Collections.emptyList());
        }

        static DispatchPlan valid(List<DispatchItem> items) {
            return new DispatchPlan(false, items);
        }

        public boolean isInvalid() {
            return invalid;
        }

        public List<DispatchItem> getItems() {
            return items;
        }
    }

    public static final class DispatchItem {
        private final Long messageId;
        private final String ackMessageId;
        private final String body;
        private final boolean duplicate;
        private final boolean callback;
        private final boolean ack;

        DispatchItem(Long messageId, String ackMessageId, String body,
                     boolean duplicate, boolean callback, boolean ack) {
            this.messageId = messageId;
            this.ackMessageId = ackMessageId;
            this.body = body;
            this.duplicate = duplicate;
            this.callback = callback;
            this.ack = ack;
        }

        public Long getMessageId() {
            return messageId;
        }

        public String getAckMessageId() {
            return ackMessageId;
        }

        public String getBody() {
            return body;
        }

        public boolean isDuplicate() {
            return duplicate;
        }

        public boolean shouldCallback() {
            return callback;
        }

        public boolean shouldAck() {
            return ack;
        }
    }
}
