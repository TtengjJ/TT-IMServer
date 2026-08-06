package com.crossoverjie.cim.route.api.vo.req;

import com.crossoverjie.cim.common.req.BaseRequest;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ChatReqVO extends BaseRequest {

    @NotNull(message = "userId 不能为空")
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = "userId", example = "1545574049323")
    private Long userId;

    @NotNull(message = "msg 不能为空")
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = "msg", example = "hello")
    private String msg;

    private List<String> batchMsg;

    /** P2P 可靠投递 messageId；群聊可为 null */
    private Long messageId;

    public ChatReqVO(Long userId, String msg, List<String> batchMsg) {
        this.userId = userId;
        this.msg = msg;
        this.batchMsg = batchMsg;
    }

    @Override
    public String toString() {
        return "GroupReqVO{userId=" + userId + ", msg='" + msg + "', messageId=" + messageId + "} " + super.toString();
    }
}
