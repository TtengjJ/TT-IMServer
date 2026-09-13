package com.crossoverjie.cim.server.api.vo.req;

import com.crossoverjie.cim.common.protocol.BaseCommand;
import com.crossoverjie.cim.common.req.BaseRequest;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * Function:
 *
 * @author crossoverJie
 *         Date: 2018/05/21 15:56
 * @since JDK 1.8
 */
@Getter
@Builder
@AllArgsConstructor
public class SendMsgReqVO extends BaseRequest {

    @Setter
    @NotNull(message = "msg 不能为空")
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = "msg", example = "hello")
    private String msg;

    @Setter
    private List<String> batchMsg;

    /**
     * 与 {@link #batchMsg} 按下标对齐的消息编号列表。
     * 两者均非空且等长时走可靠投递批量协议；不要用 properties.messageId 表示整包。
     */
    @Setter
    private List<Long> batchMessageIds;

    @NotNull(message = "userId 不能为空")
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = "userId", example = "11")
    private Long userId;

    @Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = "cmd", example = "message")
    private BaseCommand cmd;

    @Setter
    private Map<String, String> properties;

    public SendMsgReqVO() {
    }

    public SendMsgReqVO(String msg, Long userId, List<String> batchMsg, BaseCommand cmd) {
        this.msg = msg;
        this.batchMsg = batchMsg;
        this.userId = userId;
        this.cmd = cmd;
    }


    @Override
    public String toString() {
        return "SendMsgReqVO{" +
                "msg='" + msg + '\'' +
                ", batchMsg=" + batchMsg +
                ", batchMessageIds=" + batchMessageIds +
                ", userId=" + userId +
                ", cmd=" + cmd +
                ", properties=" + properties +
                '}';
    }
}
