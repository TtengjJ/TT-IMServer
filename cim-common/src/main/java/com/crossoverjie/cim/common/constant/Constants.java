package com.crossoverjie.cim.common.constant;

/**
 * Function:常量
 *
 * @author crossoverJie
 *         Date: 28/03/2018 17:41
 * @since JDK 1.8
 */
public class Constants {


    /**
     * 服务端手动 push 次数
     */
    public static final String COUNTER_SERVER_PUSH_COUNT = "counter.server.push.count";


    /**
     * 客户端手动 push 次数
     */
    public static final String COUNTER_CLIENT_PUSH_COUNT = "counter.client.push.count";

    public static class MetaKey {
        public static final String SEND_USER_ID = "sendUserId";
        public static final String SEND_USER_NAME = "sendUserName";
        public static final String RECEIVE_USER_ID = "receiveUserId";
        public static final String RECEIVE_USER_NAME = "receiveUserName";
        /** P2P 可靠投递：下行 MESSAGE/OFFLINE 与上行 MESSAGE_ACK 均携带 */
        public static final String MESSAGE_ID = "messageId";
    }

    /**
     * P2P 投递状态（offline_msg.status）。
     * <p>
     * 0 PENDING → 1 SENT → 2 ACKED；超限 → 3 FAILED。
     * 注意：历史常量 OFFLINE_MSG_DELIVERED=1 曾被误用作「已确认」，现 1 仅表示已推送待 ACK。
     */
    public static class DeliveryStatus {
        public static final int PENDING = 0;
        public static final int SENT = 1;
        public static final int ACKED = 2;
        public static final int FAILED = 3;
    }

    //从数据库读取离线消息的每次获取量
    public static final Integer FETCH_OFFLINE_MSG_LIMIT = 100;

    /** @see DeliveryStatus#PENDING */
    public static final Integer OFFLINE_MSG_PENDING = DeliveryStatus.PENDING;

    /**
     * @deprecated 语义已变更为 {@link DeliveryStatus#SENT}（已推送待确认），不再表示已 ACK
     */
    @Deprecated
    public static final Integer OFFLINE_MSG_DELIVERED = DeliveryStatus.SENT;

    public static final Integer MSG_TYPE_TEXT = 0;

    public static final Integer MSG_TYPE_IMAGE = 1;

}
