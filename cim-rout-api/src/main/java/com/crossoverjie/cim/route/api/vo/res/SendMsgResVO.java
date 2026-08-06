package com.crossoverjie.cim.route.api.vo.res;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class SendMsgResVO {
    private String msg;
    private Long messageId;

    public SendMsgResVO() {
    }

    public SendMsgResVO(String msg, Long messageId) {
        this.msg = msg;
        this.messageId = messageId;
    }

    public static SendMsgResVOBuilder builder() {
        return new SendMsgResVOBuilder();
    }

    public static final class SendMsgResVOBuilder {
        private String msg;
        private Long messageId;

        private SendMsgResVOBuilder() {
        }

        public SendMsgResVOBuilder msg(String msg) {
            this.msg = msg;
            return this;
        }

        public SendMsgResVOBuilder messageId(Long messageId) {
            this.messageId = messageId;
            return this;
        }

        public SendMsgResVO build() {
            return new SendMsgResVO(msg, messageId);
        }
    }
}