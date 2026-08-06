package com.crossoverjie.cim.route.api.vo.req;

import com.crossoverjie.cim.common.req.BaseRequest;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@EqualsAndHashCode(callSuper = true)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AckMsgReqVO extends BaseRequest {

    @NotNull
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = "messageId")
    private Long messageId;

    @NotNull
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED, description = "receive userId who ACKs")
    private Long receiveUserId;
}
