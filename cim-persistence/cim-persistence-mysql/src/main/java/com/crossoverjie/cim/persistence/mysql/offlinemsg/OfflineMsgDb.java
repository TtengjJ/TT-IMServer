package com.crossoverjie.cim.persistence.mysql.offlinemsg;

import com.crossoverjie.cim.common.constant.Constants;
import com.crossoverjie.cim.persistence.api.pojo.OfflineMsg;
import com.crossoverjie.cim.persistence.api.service.OfflineMsgStore;
import com.crossoverjie.cim.persistence.mysql.offlinemsg.mapper.OfflineMsgLastSendRecordMapper;
import com.crossoverjie.cim.persistence.mysql.offlinemsg.mapper.OfflineMsgMapper;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.crossoverjie.cim.common.constant.Constants.FETCH_OFFLINE_MSG_LIMIT;

public class OfflineMsgDb implements OfflineMsgStore {

    private final OfflineMsgMapper offlineMsgMapper;
    private final OfflineMsgLastSendRecordMapper offlineMsgLastSendRecordMapper;

    public OfflineMsgDb(OfflineMsgMapper offlineMsgMapper,
                        OfflineMsgLastSendRecordMapper offlineMsgLastSendRecordMapper) {
        this.offlineMsgMapper = offlineMsgMapper;
        this.offlineMsgLastSendRecordMapper = offlineMsgLastSendRecordMapper;
    }

    @Override
    public void save(OfflineMsg offlineMsg) {
        if (offlineMsg.getRetryCount() == null) {
            offlineMsg.setRetryCount(0);
        }
        offlineMsgMapper.insert(offlineMsg);
    }

    @Override
    public List<OfflineMsg> fetch(Long receiveUserId) {
        return offlineMsgMapper.fetchOfflineMsgsWithCursor(receiveUserId, FETCH_OFFLINE_MSG_LIMIT);
    }

    @Override
    @Deprecated
    public void markDelivered(Long receiveUserId, List<Long> messageIds) {
        if (CollectionUtils.isEmpty(messageIds)) {
            return;
        }
        for (Long messageId : messageIds) {
            markAcked(receiveUserId, messageId);
        }
    }

    @Override
    public void markSent(Long userId, List<Long> messageIds) {
        if (CollectionUtils.isEmpty(messageIds)) {
            return;
        }
        offlineMsgMapper.updateStatus(userId, messageIds, Constants.DeliveryStatus.SENT);
    }

    @Override
    public void markAcked(Long receiveUserId, Long messageId) {
        offlineMsgMapper.updateStatusByMessageId(messageId, Constants.DeliveryStatus.ACKED);
        if (receiveUserId != null) {
            offlineMsgLastSendRecordMapper.saveLatestMessageId(receiveUserId, messageId);
        }
    }

    @Override
    public void prepareRetry(Long messageId) {
        offlineMsgMapper.prepareRetry(messageId, Constants.DeliveryStatus.PENDING);
    }

    @Override
    public void markFailed(Long messageId) {
        offlineMsgMapper.updateStatusByMessageId(messageId, Constants.DeliveryStatus.FAILED);
    }

    @Override
    public Optional<OfflineMsg> findByMessageId(Long messageId) {
        return Optional.ofNullable(offlineMsgMapper.findByMessageId(messageId));
    }

    @Override
    public List<OfflineMsg> listNeedRetry(int maxRetry, int limit) {
        List<OfflineMsg> list = offlineMsgMapper.listNeedRetry(maxRetry, limit,
                Constants.DeliveryStatus.PENDING, Constants.DeliveryStatus.SENT);
        return list == null ? Collections.emptyList() : list;
    }
}
