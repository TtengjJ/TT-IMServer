package com.crossoverjie.cim.route.factory;

import com.crossoverjie.cim.common.constant.Constants;
import com.crossoverjie.cim.common.exception.CIMException;
import com.crossoverjie.cim.common.util.SnowflakeIdWorker;
import com.crossoverjie.cim.persistence.api.pojo.OfflineMsg;
import com.crossoverjie.cim.persistence.api.vo.req.SaveOfflineMsgReqVO;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import static com.crossoverjie.cim.common.constant.Constants.MSG_TYPE_TEXT;
import static com.crossoverjie.cim.common.constant.Constants.OFFLINE_MSG_PENDING;

@Service
public class OfflineMsgFactory {

    private final SnowflakeIdWorker idWorker;

    public OfflineMsgFactory(SnowflakeIdWorker idWorker) {
        this.idWorker = idWorker;
    }

    public OfflineMsg createFromVo(SaveOfflineMsgReqVO vo) {
        try {
            Long msgId = idWorker.nextId();
            Map<String, String> properties = createPropertiesMap(vo, msgId);
            return OfflineMsg.builder()
                    .messageId(msgId)
                    .receiveUserId(vo.getReceiveUserId())
                    .content(vo.getMsg())
                    .messageType(MSG_TYPE_TEXT)
                    .status(OFFLINE_MSG_PENDING)
                    .retryCount(0)
                    .createdAt(LocalDateTime.now())
                    .properties(properties)
                    .build();
        } catch (Exception e) {
            throw new CIMException("Failed to create OfflineMsg from SaveOfflineMsgReqVO", e);
        }
    }

    private Map<String, String> createPropertiesMap(SaveOfflineMsgReqVO vo, Long msgId) {
        Map<String, String> properties = new HashMap<>();
        Map<String, String> sourceProps = vo.getProperties();
        if (sourceProps != null) {
            properties.put(Constants.MetaKey.SEND_USER_ID, sourceProps.get(Constants.MetaKey.SEND_USER_ID));
            properties.put(Constants.MetaKey.SEND_USER_NAME, sourceProps.get(Constants.MetaKey.SEND_USER_NAME));
            properties.put(Constants.MetaKey.RECEIVE_USER_ID, sourceProps.get(Constants.MetaKey.RECEIVE_USER_ID));
        }
        properties.put(Constants.MetaKey.MESSAGE_ID, String.valueOf(msgId));
        return properties;
    }
}
